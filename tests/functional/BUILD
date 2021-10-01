python_library(
    name="lib/aio_utils/tests/functional",
    dependencies=[
        "3rdparty/python:aiopg",
        "3rdparty/python:SQLAlchemy",
        "lib/aio_utils/src",
        "lib/functional_test_utils/src",
        "lib/python_core/src",
        "lib/tables/src",
        "lib/unit_test_utils/src",
    ],
    sources=["**/*.py"],
    tags={"functional", "lib", "python", "tests"},
)
python_tests(
    dependencies=[":lib/aio_utils/tests/functional"],
    sources=["**/*.py"],
    tags={"functional", "lib", "python", "tests"},
    coverage=["ns_aio_utils"],
)
python_binary(
    name="unittest",
    entry_point="unittest",
    dependencies=[":lib/aio_utils/tests/functional"],
    zip_safe=False,
)
