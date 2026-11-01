"""lodge104-memberdata Lambda handler.

Pulls the full Lodge 104 membership roster from the LodgeMaster OData API
and writes it into the lodge104-member_data DynamoDB table (MEMBER_DATA_TABLE_NAME).
That table's stream is the first stage of the pipeline consumed by the
separate lodge104-mailerlite-sync stack:
https://github.com/Lodge104/sync.mailerlite.lambda.lodge104.net

Triggered hourly by a dedicated EventBridge schedule rule (see
template.yaml). The LodgeMaster portal session cookies this function
depends on (in KEYS_TABLE_NAME) are refreshed on the same cadence,
out-of-band, by the separately-owned lodge104-lodgemaster-scrapper
function.
"""

import json
import os

import boto3
import urllib3

MEMBER_DATA_TABLE_NAME = os.environ["MEMBER_DATA_TABLE_NAME"]
KEYS_TABLE_NAME = os.environ["KEYS_TABLE_NAME"]
LODGEMASTER_BASE_URL = os.environ["LODGEMASTER_BASE_URL"]

# Names of the session-cookie items this function reads from
# KEYS_TABLE_NAME. Refreshed hourly, out-of-band, by the
# lodge104-lodgemaster-scrapper function (a separate stack) logging into
# the LodgeMaster portal. Not secrets themselves -- the cookie *values*
# stored under these item names are -- so they are safe to keep as code
# constants rather than parameters.
COOKIE_ITEM_NAMES = ("XSRF-TOKEN", "ai_session", "OA.LM.Lodge.Auth", "ai_user")

# Path and query string for the LodgeMaster MemberGrid OData endpoint,
# appended to LODGEMASTER_BASE_URL. Preserved byte-for-byte from the
# pre-SAM version of this function.
#
# NOTE: "%24skip=10&%24top=10000" fetches at most 10,000 rows starting at
# row 11 in BsaPersonId-descending order. The table this writes to
# currently holds ~10.1k members, so a single unpaginated request like
# this one can miss a small number of the newest/oldest rows. This is a
# pre-existing data-completeness limitation, preserved here rather than
# silently changed as part of the SAM migration; fixing it (e.g. by
# paging through $skip) is tracked separately.
MEMBER_GRID_PATH = (
    "/odata/MemberGrid?%24orderby=BsaPersonId%20desc&%24skip=10&%24top=10000"
    "&%24select=Positions%2CEmailPrimaryBounced%2CPhonePrimaryBounced%2CBrotherhoodDate"
    "%2CBrotherhoodEligibleDate%2CBsaRegistrationActive%2CBsaRegistrationOverridden"
    "%2CBsaPersonId%2CBsaLastCheckDate%2CBsaLastCheckStatus%2CChapter%2CAddressPrimaryCity"
    "%2CDateOfBirth%2CDateOfDeath%2CDistrict%2CDuesInputDate%2CDuesPaidDate%2CElectionDate"
    "%2CEmailPrimaryAddress%2CEmailPrimaryType%2CEthnicBackground%2CFirstName%2CFullName"
    "%2CGender%2CEmailHomeBounced%2CEmailHomeAddress%2CLastName%2CLastUpdatedDate"
    "%2CPositionLec%2CLevel%2CEmailLodgeAddress%2CDuesYear%2CMiddleName%2CPhoneMobileNumber"
    "%2CNickname%2CEmailPrimaryNoContact%2CAddressPrimaryNoContact%2COrdealDate"
    "%2CCustomEmailParent2Address%2CEmailParentAddress%2CPhonePrimaryNumber"
    "%2CPhonePrimaryType%2CPortalAccount%2CAddressPrimaryState%2CAddressPrimaryStreet1"
    "%2CAddressPrimaryStreet2%2CSuffix%2CUnit%2CUnitCity%2CUnitDesignation%2CUnitNumber"
    "%2CUnitRepresentative%2CUnitType%2CVigilEligibleDate%2CVigilInductionDate"
    "%2CVigilNameEnglish%2CVigilNameTranslated%2CYouthProtectionCompliant"
    "%2CYouthProtectionDate%2CYouthProtectionOverridden%2CYouthAdult"
    "%2CAddressPrimaryZipCode%2COalmId&%24filter=BsaPersonId%20ne%20null"
    "&duesIncludeYears=2023%2C2049%2C2048%2C2047%2C2046%2C2045%2C2044%2C2043%2C2042"
    "%2C2041%2C2040%2C2039%2C2038%2C2037%2C2036%2C2035%2C2034%2C2033%2C2032%2C2031"
    "%2C2030%2C2029%2C2028%2C2027%2C2026%2C2025%2C2024%2C2022%2C2021%2C2020%2C2019"
    "%2C2018%2C2017%2C2016%2C2015%2C2014%2C2013%2C2012%2C2011%2C2010%2C2009%2C2008"
    "%2C2007%2C2006%2C2005%2C2004%2C2003%2C2002%2C2001%2C2000%2C1999%2C1998%2C1997"
    "%2C1993%2C1982%2C1981%2C1974"
)

# Reused across warm invocations of the same execution environment. See
# https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html.
_http = urllib3.PoolManager()
_dynamodb = boto3.resource("dynamodb")
_dynamodb_client = boto3.client("dynamodb")

# Populated lazily on first invocation of each execution environment and
# reused across warm invocations; never logged (it contains live session
# credentials).
_cached_lodgemaster_headers = None


def _build_lodgemaster_headers():
    """Builds the Cookie/X-Xsrf-Token headers from the session cookies
    lodge104-lodgemaster-scrapper maintains in KEYS_TABLE_NAME.

    Cached per execution environment: a cold start reads all four items
    once; later warm invocations reuse the result. Because the cache is
    only cleared by a fresh cold start, a cookie rotation is not picked up
    by an already-warm environment until it is recycled.
    """
    global _cached_lodgemaster_headers
    if _cached_lodgemaster_headers is not None:
        return _cached_lodgemaster_headers

    cookie_pairs = []
    xsrf_token_value = None
    for item_name in COOKIE_ITEM_NAMES:
        response = _dynamodb_client.get_item(
            TableName=KEYS_TABLE_NAME,
            Key={"name": {"S": item_name}},
        )
        item = response.get("Item")
        if item is None:
            raise RuntimeError(
                f"'{item_name}' is missing from {KEYS_TABLE_NAME}; has "
                "lodge104-lodgemaster-scrapper run recently?"
            )
        value = item["value"]["S"]
        cookie_pairs.append(f"{item_name}={value}")
        if item_name == "XSRF-TOKEN":
            xsrf_token_value = value

    _cached_lodgemaster_headers = {
        "Cookie": "; ".join(cookie_pairs) + ";",
        "X-Xsrf-Token": xsrf_token_value,
    }
    return _cached_lodgemaster_headers


def _fetch_members():
    """Fetches the current page of the LodgeMaster MemberGrid OData feed.

    Returns the list of member records (each a DynamoDB-shaped dict, e.g.
    {"BsaPersonId": {"N": "123"}, ...}) ready to write directly with
    batch_writer().
    """
    url = f"{LODGEMASTER_BASE_URL}{MEMBER_GRID_PATH}"
    response = _http.request("GET", url, headers=_build_lodgemaster_headers())
    if response.status != 200:
        raise RuntimeError(
            f"LodgeMaster MemberGrid request failed with status {response.status}"
        )
    return json.loads(response.data)["value"]


def lambda_handler(event, context):
    table = _dynamodb.Table(MEMBER_DATA_TABLE_NAME)
    members = _fetch_members()

    with table.batch_writer() as batch:
        for member in members:
            batch.put_item(Item=member)

    return {"membersSynced": len(members)}
