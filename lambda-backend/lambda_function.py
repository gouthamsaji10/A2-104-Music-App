from __future__ import annotations

import base64
import json
import os
from decimal import Decimal
from urllib.parse import parse_qs, unquote

# Environment variables allow the same code to be reused if table names change in AWS
MUSIC_TABLE = os.environ.get("MUSIC_TABLE", "music")
LOGIN_TABLE = os.environ.get("LOGIN_TABLE", "login")
SUBSCRIPTIONS_TABLE = os.environ.get("SUBSCRIPTIONS_TABLE", "subscriptions")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# DynamoDB objects are cached between Lambda invocations to reduce repeated setup time
_dynamodb_resource = None
_table_cache = {}


def lambda_handler(event, context):
    try:
        method = get_http_method(event)
        path = normalize_path(event)

        # Respond to browser CORS preflight requests before any application logic runs
        if method == "OPTIONS":
            return response(200, {
                "success": True,
                "message": "CORS preflight OK",
            })

        # Basic health check used to confirm that the deployed Lambda is reachable
        if method == "GET" and path == "/health":
            return response(200, {
                "success": True,
                "message": "Python Lambda backend is working with DynamoDB",
            })

        # Authentication routes used by the login and registration pages
        if method == "POST" and path == "/register":
            return register_user(event)

        if method == "POST" and path == "/login":
            return login_user(event)

        # User management routes operate on the login table
        if method == "GET" and path == "/users":
            return scan_table(LOGIN_TABLE)

        if path.startswith("/users/"):
            email = get_path_part(path, 2)

            if method == "GET":
                return get_item(LOGIN_TABLE, {"email": email})

            if method == "PUT":
                return update_item(LOGIN_TABLE, {"email": email}, get_body_map(event), {"email"})

            if method == "DELETE":
                return delete_item(LOGIN_TABLE, {"email": email})

        # Music query route used by the main frontend search form
        if method == "GET" and path == "/music/query":
            return query_music_for_frontend(event)

        # General music CRUD routes are kept for direct API testing and maintenance
        if method == "GET" and path == "/music":
            params = get_query_params(event)
            artist = get_query_param(params, "artist")

            if artist:
                return query_music_by_artist(artist)

            return scan_table(MUSIC_TABLE)

        if method == "POST" and path == "/music":
            return create_music(event)

        if path.startswith("/music/"):
            artist = get_path_part(path, 2)
            song_id = get_path_part(path, 3)

            if not artist or not song_id:
                return response(400, {
                    "success": False,
                    "message": "Both artist and song_id are required in the path",
                })

            music_key = {
                "artist": artist,
                "song_id": song_id,
            }

            if method == "GET":
                return get_item(MUSIC_TABLE, music_key)

            if method == "PUT":
                return update_item(MUSIC_TABLE, music_key, get_body_map(event), {"artist", "song_id"})

            if method == "DELETE":
                return delete_item(MUSIC_TABLE, music_key)

        # Subscription routes match the requests made by the frontend after login
        if method == "GET" and path == "/subscriptions":
            params = get_query_params(event)
            email = get_query_param(params, "email")

            if is_blank(email):
                return response(400, {
                    "success": False,
                    "message": "email is required",
                })

            return query_subscriptions_for_frontend(email)

        if method == "DELETE" and path == "/subscriptions":
            params = get_query_params(event)
            email = get_query_param(params, "email")
            song_id = get_query_param(params, "song_id")

            if is_blank(email) or is_blank(song_id):
                return response(400, {
                    "success": False,
                    "message": "email and song_id are required",
                })

            get_table(SUBSCRIPTIONS_TABLE).delete_item(Key={
                "email": email,
                "song_id": song_id,
            })

            return response(200, {
                "success": True,
                "message": "Subscription removed successfully",
            })

        if method == "POST" and path == "/subscriptions":
            return create_subscription(event)

        if method == "GET" and path.startswith("/subscriptions/"):
            return query_subscriptions_by_email(get_path_part(path, 2))

        if method == "DELETE" and path.startswith("/subscriptions/"):
            email = get_path_part(path, 2)
            song_id = get_path_part(path, 3)

            if not email or not song_id:
                return response(400, {
                    "success": False,
                    "message": "Both email and song_id are required in the path",
                })

            return delete_item(SUBSCRIPTIONS_TABLE, {
                "email": email,
                "song_id": song_id,
            })

        return response(404, {
            "success": False,
            "message": "Route not found",
            "method": method,
            "path": path,
        })

    except Exception as error:
        # Return a controlled error response instead of exposing a raw Lambda failure
        return response(500, {
            "success": False,
            "message": "Internal server error",
            "details": str(error),
        })


def get_table(table_name):
    # Reuse table objects when the Lambda container is reused by AWS
    if table_name not in _table_cache:
        _table_cache[table_name] = get_dynamodb_resource().Table(table_name)

    return _table_cache[table_name]


def get_dynamodb_resource():
    global _dynamodb_resource

    if _dynamodb_resource is None:
        import boto3

        _dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)

    return _dynamodb_resource


def register_user(event):
    # Registration inserts a new user only when the email does not already exist
    body = get_body_map(event)

    if "email" not in body:
        return response(400, {
            "success": False,
            "message": "email is required",
        })

    try:
        get_table(LOGIN_TABLE).put_item(
            Item=to_dynamodb_item(body),
            ConditionExpression="attribute_not_exists(email)",
        )

        return response(201, {
            "success": True,
            "message": "Registration successful",
        })

    except Exception as error:
        if is_conditional_check_failed(error):
            return response(409, {
                "success": False,
                "message": "The email already exists",
            })

        raise


def login_user(event):
    # Login validates the submitted password against the record stored in DynamoDB
    body = get_body_map(event)

    email = string_value(body.get("email"))
    password = string_value(body.get("password"))

    if is_blank(email):
        return response(400, {
            "success": False,
            "message": "email is required",
        })

    result = get_table(LOGIN_TABLE).get_item(Key={"email": email})
    item = result.get("Item")

    if not item:
        return invalid_login_response()

    stored_password = item.get("password")

    if stored_password is None or password is None or password != str(stored_password):
        return invalid_login_response()

    return response(200, {
        "success": True,
        "message": "Login successful",
        "email": email,
        "user_name": string_value(item.get("user_name")) or email,
    })


def invalid_login_response():
    return response(401, {
        "success": False,
        "message": "email or password is invalid",
    })


def create_music(event):
    # Music records require the DynamoDB partition and sort key fields
    body = get_body_map(event)

    if "artist" not in body or "song_id" not in body:
        return response(400, {
            "success": False,
            "message": "artist and song_id are required",
        })

    get_table(MUSIC_TABLE).put_item(Item=to_dynamodb_item(body))

    return response(201, {
        "success": True,
        "message": "Music item created successfully",
    })


def create_subscription(event):
    # A user can subscribe to a song only once, enforced by the conditional write
    body = get_body_map(event)

    if "email" not in body or "song_id" not in body:
        return response(400, {
            "success": False,
            "message": "email and song_id are required",
        })

    try:
        get_table(SUBSCRIPTIONS_TABLE).put_item(
            Item=to_dynamodb_item(body),
            ConditionExpression="attribute_not_exists(email) AND attribute_not_exists(song_id)",
        )

        return response(201, {
            "success": True,
            "message": "Song subscribed successfully",
        })

    except Exception as error:
        if is_conditional_check_failed(error):
            return response(409, {
                "success": False,
                "message": "Song is already subscribed",
            })

        raise


def query_music_for_frontend(event):
    # The frontend supports partial searches across title, year, artist, and album
    params = get_query_params(event)

    title = get_query_param(params, "title")
    year = get_query_param(params, "year")
    artist = get_query_param(params, "artist")
    album = get_query_param(params, "album")

    songs = scan_all(MUSIC_TABLE)
    filtered_songs = []

    for song in songs:
        matches_title = is_blank(title) or contains_ignore_case(song.get("title"), title)
        matches_year = is_blank(year) or contains_ignore_case(song.get("year"), year)
        matches_artist = is_blank(artist) or contains_ignore_case(song.get("artist"), artist)
        matches_album = is_blank(album) or contains_ignore_case(song.get("album"), album)

        if matches_title and matches_year and matches_artist and matches_album:
            filtered_songs.append(song)

    return response(200, {
        "success": True,
        "songs": filtered_songs,
    })


def query_music_by_artist(artist):
    result = query_all(
        MUSIC_TABLE,
        "#artist = :artist",
        {"#artist": "artist"},
        {":artist": artist},
    )

    return response(200, result)


def query_subscriptions_for_frontend(email):
    subscriptions = get_subscriptions(email)

    return response(200, {
        "success": True,
        "subscriptions": subscriptions,
    })


def query_subscriptions_by_email(email):
    return response(200, get_subscriptions(email))


def get_subscriptions(email):
    # Subscription records are enriched later with music details for display
    subscription_items = query_all(
        SUBSCRIPTIONS_TABLE,
        "#email = :email",
        {"#email": "email"},
        {":email": email},
    )

    return [enrich_subscription_with_music(item) for item in subscription_items]


def scan_table(table_name):
    return response(200, scan_all(table_name))


def get_item(table_name, key):
    result = get_table(table_name).get_item(Key=key)
    item = result.get("Item")

    if not item:
        return response(404, {
            "success": False,
            "message": "Item not found",
        })

    return response(200, item_to_json(item))


def update_item(table_name, key, body, key_attributes):
    # Build a DynamoDB update expression while protecting primary key attributes
    names = {}
    values = {}
    update_parts = []

    for index, (attribute_name, value) in enumerate(body.items()):
        if attribute_name in key_attributes:
            continue

        name_placeholder = f"#field{index}"
        value_placeholder = f":value{index}"

        names[name_placeholder] = attribute_name
        values[value_placeholder] = to_dynamodb_value(value)
        update_parts.append(f"{name_placeholder} = {value_placeholder}")

    if not update_parts:
        return response(400, {
            "success": False,
            "message": "No updatable fields provided",
        })

    result = get_table(table_name).update_item(
        Key=key,
        UpdateExpression="SET " + ", ".join(update_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
        ReturnValues="ALL_NEW",
    )

    return response(200, {
        "success": True,
        "message": "Item updated successfully",
        "item": item_to_json(result.get("Attributes", {})),
    })


def delete_item(table_name, key):
    get_table(table_name).delete_item(Key=key)

    return response(200, {
        "success": True,
        "message": "Item deleted successfully",
    })


def enrich_subscription_with_music(subscription_item):
    # The subscription table stores the relationship, the music table stores display data
    subscription = item_to_json(subscription_item)
    artist = string_value(subscription_item.get("artist"))
    song_id = string_value(subscription_item.get("song_id"))

    if not artist or not song_id:
        return subscription

    music_result = get_table(MUSIC_TABLE).get_item(Key={
        "artist": artist,
        "song_id": song_id,
    })
    music_item = music_result.get("Item")

    if not music_item:
        return subscription

    subscription.update(item_to_json(music_item))
    subscription["email"] = string_value(subscription_item.get("email"))

    return subscription


def scan_all(table_name):
    table = get_table(table_name)
    items = []
    request = {}

    while True:
        result = table.scan(**request)
        items.extend(item_to_json(item) for item in result.get("Items", []))

        last_key = result.get("LastEvaluatedKey")
        if not last_key:
            break

        request["ExclusiveStartKey"] = last_key

    return items


def query_all(table_name, key_condition_expression, attribute_names, attribute_values):
    table = get_table(table_name)
    items = []
    request = {
        "KeyConditionExpression": key_condition_expression,
        "ExpressionAttributeNames": attribute_names,
        "ExpressionAttributeValues": attribute_values,
    }

    while True:
        result = table.query(**request)
        items.extend(item_to_json(item) for item in result.get("Items", []))

        last_key = result.get("LastEvaluatedKey")
        if not last_key:
            break

        request["ExclusiveStartKey"] = last_key

    return items


def get_body_map(event):
    # API Gateway passes request bodies as strings, with optional base64 encoding
    body = event.get("body")

    if body is None or str(body).strip() == "":
        return {}

    if event.get("isBase64Encoded") is True:
        body = base64.b64decode(body).decode("utf-8")

    parsed = json.loads(body)

    if parsed is None:
        return {}

    if not isinstance(parsed, dict):
        raise ValueError("Request body must be a JSON object")

    return parsed


def to_dynamodb_item(body):
    return {key: to_dynamodb_value(value) for key, value in body.items()}


def to_dynamodb_value(value):
    # DynamoDB requires Decimal for JSON float values instead of Python float
    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, list):
        return [to_dynamodb_value(item) for item in value]

    if isinstance(value, dict):
        return {key: to_dynamodb_value(item) for key, item in value.items()}

    return value


def item_to_json(item):
    return {key: json_ready_value(value) for key, value in item.items()}


def json_ready_value(value):
    # Convert DynamoDB Decimal values back into JSON-friendly strings
    if isinstance(value, Decimal):
        number = value.normalize()

        if number == number.to_integral():
            return str(number.quantize(Decimal(1)))

        return format(number, "f")

    if isinstance(value, list):
        return [json_ready_value(item) for item in value]

    if isinstance(value, dict):
        return {key: json_ready_value(item) for key, item in value.items()}

    return value


def get_http_method(event):
    # Support both REST API events and HTTP API v2 events
    method = event.get("httpMethod")

    if not method:
        method = event.get("requestContext", {}).get("http", {}).get("method")

    return str(method or "").upper()


def normalize_path(event):
    # Remove trailing slashes and stage prefixes so routing stays predictable
    path = event.get("path") or event.get("rawPath") or "/"

    stage = event.get("requestContext", {}).get("stage")
    if stage and path.startswith(f"/{stage}/"):
        path = path[len(stage) + 1:]

    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]

    return path or "/"


def get_path_part(path, index):
    parts = path.split("/")

    if index >= len(parts):
        return None

    return unquote(parts[index])


def get_query_params(event):
    # Prefer parsed query parameters, with raw query string parsing as a fallback
    params = event.get("queryStringParameters") or {}

    if params:
        return {key: value for key, value in params.items() if value is not None}

    raw_query = event.get("rawQueryString") or ""
    parsed = parse_qs(raw_query, keep_blank_values=True)

    return {key: values[-1] if values else "" for key, values in parsed.items()}


def get_query_param(params, key):
    return params.get(key, "")


def is_blank(value):
    return value is None or str(value).strip() == ""


def contains_ignore_case(field_value, search_value):
    if field_value is None or search_value is None:
        return False

    return str(search_value).lower() in str(field_value).lower()


def string_value(value):
    if value is None:
        return None

    return str(value)


def is_conditional_check_failed(error):
    code = getattr(error, "response", {}).get("Error", {}).get("Code")
    return code == "ConditionalCheckFailedException"


def response(status_code, body):
    # All responses follow the API Gateway proxy format expected by the frontend
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body, default=str),
    }
