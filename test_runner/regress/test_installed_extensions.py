from logging import info

from fixtures.neon_fixtures import NeonEnv


def test_installed_extensions(neon_simple_env: NeonEnv):
    """basic test for the endpoint that returns the list of installed extensions"""

    env = neon_simple_env

    env.create_branch("test_installed_extensions")

    endpoint = env.endpoints.create_start("test_installed_extensions")

    endpoint.safe_psql("CREATE DATABASE test_installed_extensions")
    endpoint.safe_psql("CREATE DATABASE test_installed_extensions_2")

    client = endpoint.http_client()
    res = client.installed_extensions()

    info("Extensions list: %s", res)
    info("Extensions: %s", res["extensions"])
    # 'plpgsql' is a default extension that is always installed.
    assert any(
        ext["extname"] == "plpgsql" and ext["versions"] == ["1.0"] for ext in res["extensions"]
    ), "The 'plpgsql' extension is missing"

    # check that the neon_test_utils extension is not installed
    assert not any(
        ext["extname"] == "neon_test_utils" for ext in res["extensions"]
    ), "The 'neon_test_utils' extension is installed"

    pg_conn = endpoint.connect(dbname="test_installed_extensions")
    with pg_conn.cursor() as cur:
        cur.execute("CREATE EXTENSION neon_test_utils")
        cur.execute(
            "SELECT default_version FROM pg_available_extensions WHERE name = 'neon_test_utils'"
        )
        res = cur.fetchone()
        neon_test_utils_version = res[0]

    with pg_conn.cursor() as cur:
        cur.execute("CREATE EXTENSION neon version '1.1'")

    pg_conn_2 = endpoint.connect(dbname="test_installed_extensions_2")
    with pg_conn_2.cursor() as cur:
        cur.execute("CREATE EXTENSION neon version '1.2'")

    res = client.installed_extensions()

    info("Extensions list: %s", res)
    info("Extensions: %s", res["extensions"])

    # check that the neon_test_utils extension is installed only in 1 database
    # and has the expected version
    assert any(
        ext["extname"] == "neon_test_utils"
        and ext["versions"] == [neon_test_utils_version]
        and ext["n_databases"] == 1
        for ext in res["extensions"]
    )

    # check that the plpgsql extension is installed in all databases
    # this is a default extension that is always installed
    assert any(ext["extname"] == "plpgsql" and ext["n_databases"] == 4 for ext in res["extensions"])

    # check that the neon extension is installed and has expected versions
    for ext in res["extensions"]:
        if ext["extname"] == "neon":
            assert ext["n_databases"] == 2
            ext["versions"].sort()
            assert ext["versions"] == ["1.1", "1.2"]

    with pg_conn.cursor() as cur:
        cur.execute("ALTER EXTENSION neon UPDATE TO '1.3'")

    res = client.installed_extensions()

    info("Extensions list: %s", res)
    info("Extensions: %s", res["extensions"])

    # check that the neon_test_utils extension is updated
    for ext in res["extensions"]:
        if ext["extname"] == "neon":
            assert ext["n_databases"] == 2
            ext["versions"].sort()
            assert ext["versions"] == ["1.2", "1.3"]
