from logging import info
from fixtures.neon_fixtures import NeonEnv

# basic test for the endpoint that returns the list of installed extensions
def test_extensions_stats(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_extensions_stats", "empty")

    endpoint = env.endpoints.create_start("test_extensions_stats")
    endpoint.safe_psql("CREATE DATABASE test_extensions_stats")

    client = endpoint.http_client()
    res = client.extensions()

    info("Extensions list: %s", res)
    info("Extensions: %s", res["extensions"])
    # 'plpgsql' is a default extension that is always installed.
    assert any(
        ext["extname"] == "plpgsql" and ext["highest_version"] == "1.0" for ext in res["extensions"]
    ), "The 'plpgsql' extension is missing"

    # check that the neon_test_utils extension is not installed
    assert not any(
        ext["extname"] == "neon_test_utils" for ext in res["extensions"]
    ), "The 'neon_test_utils' extension is installed"

    pg_conn = endpoint.connect(dbname="test_extensions_stats")
    with pg_conn.cursor() as cur:
        cur.execute("CREATE EXTENSION neon_test_utils")

    res = client.extensions()

    info("Extensions list: %s", res)
    info("Extensions: %s", res["extensions"])

    # check that the neon_test_utils extension is installed
    assert any(
        ext["extname"] == "neon_test_utils" and ext["highest_version"] == "1.3" for ext in res["extensions"]
    ), "The 'neon_test_utils' extension is missing"
