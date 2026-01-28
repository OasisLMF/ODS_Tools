import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--keep-output", action="store_true", help="Keep the output from the tests."
    )


@pytest.fixture
def keep_output(request):
    return request.config.getoption("--keep-output")
