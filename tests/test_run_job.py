from unittest.mock import patch

import run_job


def _fake_result():
    return type(
        "R",
        (),
        {
            "run_id": "abc",
            "run_date": "2026-05-27",
            "rows_loaded": 10,
            "services_changed": 2,
            "services_skipped": 198,
            "elapsed_s": 1.2,
        },
    )()


def test_run_job_success_returns_0():
    with (
        patch("run_job.bq_setup.ensure_dataset_and_tables"),
        patch("run_job.loader.run_load", return_value=_fake_result()),
    ):
        rc = run_job.main([])
    assert rc == 0


def test_run_job_setup_failure_returns_1():
    with (
        patch("run_job.bq_setup.ensure_dataset_and_tables", side_effect=RuntimeError("ddl fail")),
        patch("run_job.loader.run_load") as mock_load,
    ):
        rc = run_job.main([])
    assert rc == 1
    mock_load.assert_not_called()


def test_run_job_load_failure_returns_1():
    with (
        patch("run_job.bq_setup.ensure_dataset_and_tables"),
        patch("run_job.loader.run_load", side_effect=RuntimeError("api down")),
    ):
        rc = run_job.main([])
    assert rc == 1


def test_run_job_passes_force_and_service_filter():
    with (
        patch("run_job.bq_setup.ensure_dataset_and_tables"),
        patch("run_job.loader.run_load", return_value=_fake_result()) as mock_load,
    ):
        rc = run_job.main(["--force", "--service-filter", "AmazonEC2,AmazonRDS"])
    assert rc == 0
    settings_arg = mock_load.call_args.kwargs["settings"]
    assert settings_arg.aws_service_filter == "AmazonEC2,AmazonRDS"
    assert mock_load.call_args.kwargs["force"] is True
