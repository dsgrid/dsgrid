from dsgrid.config.registration_models import (
    RegistrationJournal,
    RegistrationModel,
    SubmittedDatasetsJournal,
)
from dsgrid.tests.common import TEST_EFS_REGISTRATION_FILE


def test_filter_by_journal():
    registration = RegistrationModel.from_file(TEST_EFS_REGISTRATION_FILE)
    # The project and one dataset was registered. Registration of second dataset failed.
    journal = RegistrationJournal(
        registered_projects=["test_efs"],
        registered_datasets=["test_efs_comstock_unpivoted"],
        submitted_datasets=[],
    )
    assert journal.has_entries()
    registration2 = registration.filter_by_journal(journal)
    assert not registration2.projects
    assert len(registration2.datasets) == 1
    assert registration2.datasets[0].dataset_id == "test_efs_comstock"
    assert len(registration2.dataset_submissions) == 2

    # Everything worked on the second try.
    journal2 = RegistrationJournal(
        registered_projects=["test_efs"],
        registered_datasets=["test_efs_comstock_unpivoted", "test_efs_comstock"],
        submitted_datasets=[
            SubmittedDatasetsJournal(
                dataset_id="test_efs_comstock_unpivoted",
                project_id="test_efs",
            ),
            SubmittedDatasetsJournal(
                dataset_id="test_efs_comstock",
                project_id="test_efs",
            ),
        ],
    )
    registration3 = registration.filter_by_journal(journal2)
    assert not registration3.projects
    assert not registration3.datasets
    assert not registration3.dataset_submissions
