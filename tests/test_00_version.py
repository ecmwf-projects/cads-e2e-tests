import cads_e2e_tests


def test_version() -> None:
    assert cads_e2e_tests.__version__ != "999"
