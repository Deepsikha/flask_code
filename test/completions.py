"""
Tests for completions related stuffs
"""
import json
import time
from copy import deepcopy
import re
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tests.utils.helpers import *


def test_delete_single_completion(browser):
    browser.get(f"{ANNOTATIONLAB_URL}/#/projects")
    project_name=unique_project_name("test_delete_single_completion")
    create_project(browser, project_name)
    create_sample_task(browser, project_name)
    create_task(browser, project_name)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "anticon-plus"))
    )
    ls_submit_btn = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ls-submit-btn"))
    )
    browser.execute_script("arguments[0].click();", ls_submit_btn)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ant-btn-dangerous"))
    ).click()
    popover_content = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ant-popover-content"))
    )
    WebDriverWait(popover_content, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ant-btn-dangerous"))
    ).click()
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ls-skip-btn"))
    )
    assert (
        "ID 1"
        in browser.find_element_by_xpath(
            "//li[contains(@class, 'Completions_completion_deleted')]"
        ).text
    )
    delete_project(browser, project_name)


def test_import_completions_task(browser):
    project_name = unique_project_name("test_import_completions")
    create_project(browser, project_name)
    # Add collaborate to team as annotator
    members_info = {"collaborate": ["Annotator"]}
    browser.get(
        f"{ANNOTATIONLAB_URL}/#/project/{project_name}/setup#projectTeam"
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.url_contains("/setup#projectTeam")
    )
    add_to_team(browser, members_info)
    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/import")
    WebDriverWait(browser, DRIVER_TIMEOUT).until(EC.url_contains("/import"))
    task = [
        {
            "completions": [
                {
                    "created_ago": "2020-07-18T06:17:12.545Z",
                    "created_username": "admin",
                    "honeypot": False,
                    "id": 1,
                    "lead_time": 3.491,
                    "result": [
                        {
                            "from_name": "label",
                            "id": "w7PVuIWOwN",
                            "source": "$text",
                            "to_name": "text",
                            "type": "labels",
                            "value": {
                                "end": 34,
                                "labels": ["MONEY"],
                                "start": 26,
                                "text": "yourself",
                            },
                        }
                    ],
                },
                {
                    "created_ago": "2020-07-18T06:17:16.951Z",
                    "created_username": "collaborate",
                    "honeypot": False,
                    "id": 2,
                    "lead_time": 2.666,
                    "result": [
                        {
                            "from_name": "label",
                            "id": "Xu_IfgBaOS",
                            "source": "$text",
                            "to_name": "text",
                            "type": "labels",
                            "value": {
                                "end": 13,
                                "labels": ["ORG"],
                                "start": 8,
                                "text": "faith",
                            },
                        }
                    ],
                },
            ],
            "predictions": [],
            "created_at": "2020-11-24 04:55:33",
            "created_by": "admin",
            "data": {
                "text": "To have faith is to trust yourself to the water",
                "title": "",
            },
        },
        {
            "completions": [
                {
                    "created_ago": "2020-07-18T06:17:12.545Z",
                    "created_username": "admin",
                    "honeypot": False,
                    "id": 1,
                    "lead_time": 3.491,
                    "result": [
                        {
                            "from_name": "label",
                            "id": "w7PVuIWOwN",
                            "source": "$text",
                            "to_name": "text",
                            "type": "labels",
                            "value": {
                                "end": 34,
                                "labels": ["MONEY"],
                                "start": 26,
                                "text": "yourself",
                            },
                        }
                    ],
                }
            ],
            "predictions": [],
            "created_at": "2020-11-24 04:55:33",
            "created_by": "admin",
            "data": {
                "text": "To have faith is to trust yourself to the water. The patient is a pleasant 17-year-old gentleman who was playing basketball today in gym. Two hours prior to presentation, he started to fall and someone stepped on his ankle and kind of twisted his right ankle and he cannot bear weight on it now. It hurts to move or bear weight. No other injuries noted. He does not think he has had injuries to his ankle in the past. He was given adderall and accutane. He does not think he has had injuries to his ankle in the past. He was given adderall and accutane.",
                "title": "",
            },
        },
    ]

    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.ID, "url-input"))
    ).send_keys(json.dumps(task))
    browser.find_element_by_id("url-button").click()
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (By.PARTIAL_LINK_TEXT, "Explore Tasks")
        )
    ).click()
    wait_for_task_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.PARTIAL_LINK_TEXT, "Task 1"))
    )
    assert (
        "Assigned to collaborate"
        in WebDriverWait(browser, DRIVER_TIMEOUT)
        .until(
            EC.visibility_of_element_located(
                (By.XPATH, '//*[@id="tasks"]/tbody')
            )
        )
        .text
    )

    delete_project(browser, project_name)


def test_import_completion_with_missing_created_ago(browser):
    project_name = unique_project_name("test_import_completion_with_missing_created_ago")
    task = {
        "completions": [
            {
                "created_username": "admin",
                "honeypot": "false",
                "id": 1,
                "lead_time": 3.491,
                "result": [
                    {
                        "from_name": "label",
                        "id": "w7PVuIWOwN",
                        "source": "$text",
                        "to_name": "text",
                        "type": "labels",
                        "value": {
                            "end": 34,
                            "labels": ["MONEY"],
                            "start": 26,
                            "text": "yourself",
                        },
                    }
                ],
            }
        ],
        "predictions": [],
        "created_at": "2020-11-24 04:55:33",
        "created_by": "admin",
        "data": {
            "text": "To have faith is to trust yourself to the water",
            "title": "",
        },
        "id": 1,
    }
    import_completion_task(browser, project_name, task)
    browser.find_element_by_xpath('//span[contains(@class,"task-id")]').click()
    completion_ts = (
        WebDriverWait(browser, DRIVER_TIMEOUT)
        .until(
            EC.visibility_of_element_located(
                (
                    By.XPATH,
                    '//div[contains(@class,"Completions_completioncard")]//i',
                )
            )
        )
        .text
    )
    assert "Created just now" in completion_ts
    delete_project(browser, project_name)


def test_import_completion_lead_time_display(browser):
    project_name = unique_project_name("test_import_completion_lead_time_display")
    task = {
        "completions": [
            {
                "created_username": "admin",
                "created_ago": "2020-11-24T04:55:51.276Z",
                "lead_time": 17.026,
                "result": [
                    {
                        "value": {
                            "start": 8,
                            "end": 13,
                            "text": "faith",
                            "labels": ["PERSON"],
                        },
                        "id": "hfhsY870H5",
                        "from_name": "label",
                        "to_name": "text",
                        "type": "labels",
                    }
                ],
                "honeypot": "false",
                "id": 1,
            },
            {
                "created_username": "admin",
                "created_ago": "2020-11-24T04:55:33.470Z",
                "lead_time": 327.076,
                "result": [
                    {
                        "value": {
                            "start": 42,
                            "end": 47,
                            "text": "water",
                            "labels": ["FAC"],
                        },
                        "id": "iMN37nDLCH",
                        "from_name": "label",
                        "to_name": "text",
                        "type": "labels",
                    }
                ],
                "honeypot": "false",
                "id": 1001,
            },
        ],
        "predictions": [],
        "created_at": "2020-11-24 04:55:17",
        "created_by": "admin",
        "data": {
            "text": "To have faith is to trust yourself to the water",
            "title": "",
        },
        "id": 0,
    }
    import_completion_task(browser, project_name, task)
    browser.find_element_by_xpath('//span[contains(@class,"task-id")]').click()
    lead_times = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_all_elements_located(
            (By.XPATH, '//div[contains(@class,"Completions_lead_time")]')
        )
    )
    lead_times_text = [lt.text.strip() for lt in lead_times]
    assert lead_times_text == ["00:05:27", "00:00:17"]
    delete_project(browser, project_name)


def test_import_completion_created_updated_timestamps(browser):
    project_name = unique_project_name("test_import_completion_created_updated_timestamps")
    task_json = {
        "completions": [
            {
                "lead_time": 17.026,
                "created_username": "admin",
                "result": [
                    {
                        "value": {
                            "start": 8,
                            "end": 13,
                            "text": "faith",
                            "labels": ["PERSON"],
                        },
                        "id": "hfhsY870H5",
                        "from_name": "label",
                        "to_name": "text",
                        "type": "labels",
                    }
                ],
                "honeypot": "false",
                "id": 1,
            }
        ],
        "predictions": [],
        "created_at": "2020-11-24 04:55:17",
        "created_by": "admin",
        "data": {
            "text": "To have faith is to trust yourself to the water",
            "title": "",
        },
    }
    tasks = list()
    for i in range(1, 3):
        task = deepcopy(task_json)
        task["id"] = i
        task["completions"][0]["created_ago"] = "2020-11-24T04:55:51.276Z"
        if i == 2:
            task["completions"][0]["updated_at"] = "2021-01-02T04:55:51.276Z"
        tasks.append(task)

    import_completion_task(browser, project_name, tasks)

    for i in range(1, 3):
        browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/tasks")
        wait_for_task_page_load(browser)
        WebDriverWait(browser, DRIVER_TIMEOUT).until(
            EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, f"Task {i}"))
        ).click()
        wait_for_labeling_page_load(browser)
        completion_ts = (
            WebDriverWait(browser, DRIVER_TIMEOUT)
            .until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        '//div[contains(@class,"Completions_completioncard")]',
                    )
                )
            )
            .text
        )
        if i == 1:
            assert re.search(r"Created.*?ago", completion_ts)
        else:
            assert re.search(r"Updated.*?ago", completion_ts)
        ls_update_btn = WebDriverWait(browser, DRIVER_TIMEOUT).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "ls-update-btn"))
        )
        browser.execute_script("arguments[0].click();", ls_update_btn)
        completion_ts = (
            WebDriverWait(browser, DRIVER_TIMEOUT)
            .until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        '//div[contains(@class,"Completions_completioncard")]',
                    )
                )
            )
            .text
        )
        assert "Updated just now" in completion_ts

    delete_project(browser, project_name)


def test_imported_completion_delete(browser):
    task = {
        "completions": [
            {
                "created_ago": "2020-07-18T06:17:12.545Z",
                "created_username": "admin",
                "honeypot": "false",
                "id": 1,
                "lead_time": 3.491,
                "result": [
                    {
                        "from_name": "label",
                        "id": "w7PVuIWOwN",
                        "source": "$text",
                        "to_name": "text",
                        "type": "labels",
                        "value": {
                            "end": 34,
                            "labels": ["MONEY"],
                            "start": 26,
                            "text": "yourself",
                        },
                    }
                ],
            },
            {
                "created_ago": "2020-07-18T06:17:16.951Z",
                "created_username": "admin",
                "honeypot": "false",
                "id": 2,
                "lead_time": 2.666,
                "result": [
                    {
                        "from_name": "label",
                        "id": "Xu_IfgBaOS",
                        "source": "$text",
                        "to_name": "text",
                        "type": "labels",
                        "value": {
                            "end": 13,
                            "labels": ["ORG"],
                            "start": 8,
                            "text": "faith",
                        },
                    }
                ],
            },
        ],
        "predictions": [],
        "created_at": "2020-11-24 04:55:33",
        "created_by": "admin",
        "data": {
            "text": "To have faith is to trust yourself to the water",
            "title": "",
        },
        "id": 1,
    }
    project_name = unique_project_name("test_completion_delete")
    import_completion_task(browser, project_name, task)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    WebDriverWait(browser, DRIVER_TIMEOUT).until(EC.url_contains("labeling"))
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "ls-skip-btn"))
    )
    assert (
        "ID 2"
        in browser.find_element_by_xpath(
            '//div[@class="ant-collapse-content-box"]'
        ).text
    )
    browser.find_element_by_class_name("ant-btn-sm.ant-btn-dangerous").click()
    delete_button = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "//div[@class='ant-popover-buttons']"
                "/button[contains(@class,'ant-btn-dangerous')]",
            )
        )
    )
    browser.execute_script("arguments[0].click();", delete_button)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (
                By.XPATH,
                "//li[contains(@class, 'Completions_completion_deleted')]",
            )
        )
    )
    browser.refresh()
    assert (
        "ID 2"
        in browser.find_element_by_xpath(
            "//li[contains(@class, 'Completions_completion_deleted')]"
        ).text
    )
    delete_project(browser, project_name)


def test_completions_progress_message(browser):
    project_name = unique_project_name("test_completions_progress_message")
    create_project(browser, project_name)
    create_sample_task(browser, project_name)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    # labeling page right side bar all cards ["Completions", "Predictions", "Results"]
    labeling_page_card = browser.find_elements_by_class_name(
        "ant-card-bordered"
    )
    for card in labeling_page_card:
        card_text = card.text
        if "Completions" in card_text and "New completion" in card_text:
            assert "In progress" in card_text
            break

    delete_project(browser, project_name)


def test_created_ago_for_new_completion(browser):
    project_name = unique_project_name("test_created_ago_for_new_completion")
    create_project(browser, project_name)
    create_sample_task(browser, project_name)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    ls_submit_btn = browser.find_element_by_class_name("ls-submit-btn")
    browser.execute_script("arguments[0].click();", ls_submit_btn)
    labeling_page_card = browser.find_elements_by_class_name(
        "ant-card-bordered"
    )
    for card in labeling_page_card:
        card_text = card.text
        if "Completions" in card_text and "New completion" in card_text:
            assert "Created just now" in card_text
            break

    delete_project(browser, project_name)


def test_creating_a_copy_of_an_existing_completion(browser):
    project_name = unique_project_name("test_creating_a_copy_of_an_existing_completion")
    create_task_with_completion(browser, project_name)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)

    copy_button = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (
                By.XPATH,
                '//li[contains(@class,"Completions_completion_selected")]/div'
                '/div[contains(@class, "Completions_buttons")]/button[1]',
            )
        )
    )
    assert (
        len(
            browser.find_elements_by_xpath(
                "//li[contains(@class, 'Completions_completion')]"
            )
        )
        == 2
    )
    copy_button.click()
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.text_to_be_present_in_element(
            (
                By.XPATH,
                "//div[contains(@class, 'Completions_completioncard')]",
            ),
            "Created just now",
        )
    )
    assert (
        len(
            browser.find_elements_by_xpath(
                "//li[contains(@class, 'Completions_completion')]"
            )
        )
        == 3
    )
    delete_project(browser, project_name)


def test_user_can_not_change_another_user_completion(browser):
    project_name = unique_project_name("test_user_can_not_change_another_user_completion")
    task = {
        "completions": [
            {
                "created_ago": "2020-07-18T06:17:12.545Z",
                "created_username": "collaborate",
                "id": 1,
                "lead_time": 3.491,
                "result": [
                    {
                        "from_name": "label",
                        "id": "w7PVuIWOwN",
                        "source": "$text",
                        "to_name": "text",
                        "type": "labels",
                        "value": {
                            "end": 34,
                            "labels": ["MONEY"],
                            "start": 26,
                            "text": "yourself",
                        },
                    }
                ],
            },
            {
                "created_ago": "2020-07-18T06:17:16.951Z",
                "created_username": "admin",
                "id": 2,
                "lead_time": 2.666,
                "result": [
                    {
                        "from_name": "label",
                        "id": "Xu_IfgBaOS",
                        "source": "$text",
                        "to_name": "text",
                        "type": "labels",
                        "value": {
                            "end": 13,
                            "labels": ["ORG"],
                            "start": 8,
                            "text": "faith",
                        },
                    }
                ],
            },
        ],
        "predictions": [],
        "created_at": "2020-11-24 04:55:33",
        "created_by": "admin",
        "data": {
            "text": "To have faith is to trust yourself to the water",
            "title": "",
        },
        "id": 1,
    }

    import_completion_task(browser, project_name, task)

    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/tasks")
    wait_for_task_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.PARTIAL_LINK_TEXT, "Task 1"))
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)

    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "ls-submit-btn"))
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (By.XPATH, '//div[contains(@class,"Completions_completioncard")]')
        )
    )
    completions_ts = browser.find_elements_by_xpath(
        '//li[contains(@class,"Completions_completion")]'
    )
    for completion in completions_ts:
        if "ID 2" in completion.text:
            completion.click()
            WebDriverWait(completion, DRIVER_TIMEOUT).until(
                EC.visibility_of_element_located(
                    (By.CLASS_NAME, "anticon-copy")
                )
            )
            assert (
                len(
                    browser.find_elements_by_class_name(
                        "ant-btn-background-ghost"
                    )
                )
                == 1
            )
        if "ID 1" in completion.text:
            completion.click()
            WebDriverWait(completion, DRIVER_TIMEOUT).until(
                EC.visibility_of_element_located(
                    (By.CLASS_NAME, "anticon-copy")
                )
            )
            assert (
                len(
                    browser.find_elements_by_class_name(
                        "ant-btn-background-ghost"
                    )
                )
                == 0
            )
    delete_project(browser, project_name)


def test_collapse_users_completion(browser):
    project_name = unique_project_name("test_collapse_users_completion")
    create_task_with_completion(browser, project_name)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    completion_header = (
        WebDriverWait(browser, DRIVER_TIMEOUT)
        .until(
            EC.visibility_of_element_located(
                (By.XPATH, '//div[contains(@class,"ant-collapse-header")]')
            )
        )
        .text
    )
    assert "admin" in completion_header

    setup_url = (
        f"{ANNOTATIONLAB_URL}/#/project/{project_name}/setup#projectTeam"
    )
    browser.get(setup_url)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.url_contains("/setup#projectTeam")
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "txtuser"))
    )

    # Add collaborate to team as annotator
    members_info = {"collaborate": ["Annotator"]}
    add_to_team(browser, members_info)
    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/tasks")
    wait_for_task_page_load(browser)
    assign_task_to_user(browser, "collaborate", "1")
    login_as("collaborate", "collaborate", browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Shared With Me"))
    )
    browser.find_element_by_partial_link_text("Shared With Me").click()
    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/tasks")
    wait_for_task_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    completion_header = (
        WebDriverWait(browser, DRIVER_TIMEOUT)
        .until(
            EC.visibility_of_element_located(
                (By.XPATH, '//div[contains(@class,"ant-collapse-header")]')
            )
        )
        .text
    )
    assert "collaborate" in completion_header
    login_as("admin", "admin", browser)
    delete_project(browser, project_name)


def test_switch_ground_truth(browser):
    project_name = unique_project_name("test_switch_ground_truth")
    task = {
        "completions": [
            {
                "created_ago": "2020-07-18T06:17:12.545Z",
                "created_username": "admin",
                "honeypot": True,
                "id": 1,
                "lead_time": 3.491,
                "result": [
                    {
                        "from_name": "label",
                        "id": "w7PVuIWOwN",
                        "source": "$text",
                        "to_name": "text",
                        "type": "labels",
                        "value": {
                            "end": 34,
                            "labels": ["MONEY"],
                            "start": 26,
                            "text": "yourself",
                        },
                    }
                ],
            },
            {
                "created_ago": "2020-07-18T06:17:16.951Z",
                "created_username": "admin",
                "honeypot": False,
                "id": 2,
                "lead_time": 2.666,
                "result": [
                    {
                        "from_name": "label",
                        "id": "Xu_IfgBaOS",
                        "source": "$text",
                        "to_name": "text",
                        "type": "labels",
                        "value": {
                            "end": 13,
                            "labels": ["ORG"],
                            "start": 8,
                            "text": "faith",
                        },
                    }
                ],
            },
        ],
        "predictions": [],
        "created_at": "2020-11-24 04:55:33",
        "created_by": "admin",
        "data": {
            "text": "To have faith is to trust yourself to the water",
            "title": "",
        },
        "id": 1,
    }
    import_completion_task(browser, project_name, task)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (By.XPATH, '//div[contains(@class,"Completions_completioncard")]')
        )
    )
    completions_ts = browser.find_elements_by_xpath(
        '//li[contains(@class,"Completions_completioncard")]'
    )
    for completion in completions_ts:
        if "ID 2" in completion.text:
            completion.click()
            WebDriverWait(completion, DRIVER_TIMEOUT).until(
                EC.visibility_of_element_located(
                    (By.CLASS_NAME, "anticon-star")
                )
            ).click()
            assert (
                len(
                    browser.find_elements_by_class_name(
                        "ant-btn-background-ghost"
                    )
                )
                == 0
            )
        if "ID 1" in completion.text:
            completion.click()
            WebDriverWait(completion, DRIVER_TIMEOUT).until(
                EC.visibility_of_element_located(
                    (By.CLASS_NAME, "ant-btn-background-ghost")
                )
            )
            assert (
                len(
                    browser.find_elements_by_class_name(
                        "ant-btn-background-ghost"
                    )
                )
                == 1
            )
    delete_project(browser, project_name)


def test_submit_completion_button(browser):
    project_name = unique_project_name("test_submit_completion_button")
    create_project_with_task(browser, project_name)
    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/tasks")
    wait_for_task_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "ls-submit-btn"))
    )

    element = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ls-submit-btn"))
    )
    browser.execute_script("arguments[0].click();", element)

    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "btn-submit-completion"))
    )

    element = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.ID, "btn-submit-completion"))
    )
    browser.execute_script("arguments[0].click();", element)

    modal_content = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "ant-modal-content"))
    )
    WebDriverWait(modal_content, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "ant-btn-primary"))
    )
    ok_button = WebDriverWait(modal_content, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ant-btn-primary"))
    )
    browser.execute_script("arguments[0].click();", ok_button)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ant-collapse-content-box"))
    )
    completions_ts = browser.find_elements_by_xpath(
        '//div[@class="ant-collapse-content-box"]/li'
    )
    for completion in completions_ts:
        if "ID 1" in completion.text:
            browser.execute_script("arguments[0].click();", completion)
            WebDriverWait(completion, DRIVER_TIMEOUT).until(
                EC.visibility_of_element_located(
                    (By.CLASS_NAME, "anticon-copy")
                )
            )
            assert (
                len(browser.find_elements_by_class_name("anticon-delete")) == 0
            )
    delete_project(browser, project_name)


def test_import_prediction(browser):
    browser.get(f"{ANNOTATIONLAB_URL}/#/projects")
    project_name = unique_project_name("test_prediction")
    create_project(browser, project_name)
    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/import")
    prediction = [
        {
            "predictions": [
                {
                    "created_ago": "2020-06-17T13:42:06.499Z",
                    "created_username": "admin",
                    "honeypot": False,
                    "id": 1,
                    "lead_time": 5.542,
                    "result": [
                        {
                            "from_name": "label",
                            "id": "HNmOUgR3rR",
                            "source": "$text",
                            "to_name": "text",
                            "type": "labels",
                            "value": {
                                "end": 47,
                                "labels": ["FAC"],
                                "start": 42,
                                "text": "water",
                            },
                        },
                        {
                            "from_name": "label",
                            "id": "1oDqSgNvL8",
                            "source": "$text",
                            "to_name": "text",
                            "type": "labels",
                            "value": {
                                "end": 34,
                                "labels": ["ORG"],
                                "start": 26,
                                "text": "yourself",
                            },
                        },
                    ],
                }
            ],
            "created_at": "2020-06-17 13:41:59",
            "created_by": "admin",
            "data": {
                "text": "To have faith is to trust yourself to the water. The patient is a pleasant 17-year-old gentleman who was playing basketball today in gym. Two hours prior to presentation, he started to fall and someone stepped on his ankle and kind of twisted his right ankle and he cannot bear weight on it now. It hurts to move or bear weight. No other injuries noted. He does not think he has had injuries to his ankle in the past. He was given adderall and accutane. He does not think he has had injuries to his ankle in the past. He was given adderall and accutane.",
                "title": "",
            },
            "id": 0,
        }
    ]
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.ID, "url-input"))
    ).send_keys(json.dumps(prediction))
    browser.find_element_by_id("url-button").click()
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (By.PARTIAL_LINK_TEXT, "Explore Tasks")
        )
    ).click()
    wait_for_task_page_load(browser)

    assert (
        "0 In Progress"
        in browser.find_element_by_xpath(
            '//button[contains(@value,"inprogress")]'
        ).text
    )
    assert (
        "1 Incomplete"
        in browser.find_element_by_xpath(
            '//button[contains(@value,"incomplete")]'
        ).text
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.PARTIAL_LINK_TEXT, "Task 1"))
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "ls-skip-btn"))
    )
    assert len(browser.find_elements_by_class_name("ls-submit-btn")) == 0

    # Create completion based on prediction
    copy_button = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (
                By.XPATH,
                '//li[contains(@class,"Completions_completion_selected")]/div'
                '/div[contains(@class, "Completions_buttons")]/button[1]',
            )
        )
    )
    copy_button.click()
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "ls-submit-btn"))
    )
    completions_ts = browser.find_elements_by_xpath(
        '//li[contains(@class,"Completions_completion_selected")]/div'
    )
    for completion in completions_ts:
        assert "ID 1" in completion.text

    wait_for_labeling_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ant-select-selection-item"))
    ).click()

    word_count_dropdown = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (By.CLASS_NAME, "ant-select-dropdown")
        )
    )

    WebDriverWait(word_count_dropdown, DRIVER_TIMEOUT).until(
        EC.visibility_of_all_elements_located((By.CLASS_NAME, "ant-select-item-option-content"))
    )

    for dd_value in browser.find_elements_by_class_name("ant-select-item-option-content"):
        if "50 words" in dd_value.text:
            dd_value.click()
            break

    WebDriverWait(word_count_dropdown, DRIVER_TIMEOUT).until(
        EC.invisibility_of_element_located((By.CLASS_NAME, "ant-select-open"))
    )

    assert browser.find_element_by_class_name("ant-pagination")
    delete_project(browser, project_name)


def test_import_overwrite(browser):
    project_name = unique_project_name("test_import_overwrite")
    completion = {
        "created_ago": "2020-11-24T04:55:51.276Z",
        "created_username": "admin",
        "honeypot": "false",
        "id": 1001,
        "lead_time": 7.026,
        "result": [
            {
                "from_name": "label",
                "id": "hfhsY870H5",
                "to_name": "text",
                "type": "labels",
                "value": {
                    "end": 13,
                    "labels": ["PERSON"],
                    "start": 8,
                    "text": "faith",
                },
            }
        ],
    }
    task_json = {
        "completions": [completion],
        "created_at": "2020-11-24 04:55:17",
        "created_by": "admin",
        "data": {
            "text": "To have faith is to trust yourself to the water",
            "title": "",
        },
        "id": 1,
        "predictions": [],
    }
    import_completion_task(browser, project_name, task_json)

    # donot check overwrite and import same task(same task,same id)

    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/import")

    WebDriverWait(browser, 10).until(
        EC.visibility_of_element_located((By.ID, "url-input"))
    ).send_keys(json.dumps(task_json))
    browser.find_element_by_id("url-button").click()
    WebDriverWait(browser, 10).until(
        EC.visibility_of_element_located((By.ID, "upload-dialog-msg"))
    )
    assert (
        "Tasks created:0"
        in browser.find_element_by_xpath(
            '//*[@id="upload-dialog-msg"]/h2'
        ).text
    )
    warning_msg = (
        "WARNING:1 duplicate task(s) detected and not imported. "
        "Either check the overwrite checkbox to overwrite the "
        "existing tasks or update task id in json"
    )
    assert (
        warning_msg
        in browser.find_element_by_xpath('//*[@id="upload-dialog-msg"]').text
    )

    # check overwrite and import same task(same task,same id)

    new_comp = deepcopy(completion)
    new_comp["id"] = 1002
    task_json["completions"].append(new_comp)
    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/import")
    WebDriverWait(browser, DRIVER_TIMEOUT).until(EC.url_contains("/import"))
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "overwrite"))
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.ID, "overwrite"))
    )
    overwrite = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.ID, "overwrite"))
    )
    ActionChains(browser).move_to_element(overwrite).click().perform()

    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "url-input"))
    )
    if (
        WebDriverWait(browser, DRIVER_TIMEOUT)
        .until(EC.element_to_be_clickable((By.ID, "overwrite")))
        .get_attribute("checked")
        != "true"
    ):
        checkbox = WebDriverWait(browser, DRIVER_TIMEOUT).until(
            EC.element_to_be_clickable((By.ID, "overwrite"))
        )
        browser.execute_script("arguments[0].click();", checkbox)

    input_box = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.ID, "url-input"))
    )
    ActionChains(browser).move_to_element(input_box).click().send_keys(
        json.dumps(task_json)
    ).perform()
    import_button = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.ID, "url-button"))
    )
    ActionChains(browser).move_to_element(import_button).click().perform()
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "upload-dialog"))
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (By.PARTIAL_LINK_TEXT, "Explore Tasks")
        )
    )
    explore_button = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Explore Tasks"))
    )
    assert (
        "Tasks created:0"
        in browser.find_element_by_xpath('//*[@id="upload-dialog-msg"]').text
    )
    assert (
        "Tasks updated:1"
        in browser.find_element_by_xpath('//*[@id="upload-dialog-msg"]').text
    )
    ActionChains(browser).move_to_element(explore_button).click().perform()

    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "tasks"))
    )
    tasks_xpath = '//span[contains(@class,"task-id")]'
    assert len(browser.find_elements_by_xpath(tasks_xpath)) == 1
    browser.find_element_by_xpath(tasks_xpath).click()
    comp_cards = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_all_elements_located(
            (By.XPATH, '//div[contains(@class,"Completions_completioncard")]')
        )
    )
    assert len(comp_cards) == 2

    # import different task(change text only keeping id same)

    task_json["data"]["text"] += ".."
    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/import")
    browser.find_element_by_xpath("//input[@id='overwrite']").click()
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "url-input"))
    ).send_keys(json.dumps(task_json))
    browser.find_element_by_id("url-button").click()
    explore_button = WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Explore Tasks"))
    )
    assert (
        "Tasks created:1"
        in browser.find_element_by_xpath('//*[@id="upload-dialog-msg"]').text
    )
    explore_button.click()
    wait_for_task_page_load(browser)
    tasks_xpath = '//span[contains(@class,"task-id")]'
    assert len(browser.find_elements_by_xpath(tasks_xpath)) == 2
    delete_project(browser, project_name)


def test_automatic_setting_of_ground_truth_for_first_completion(browser):
    project_name = unique_project_name(
        "test_default_ground_truth_for_first_completion"
    )
    create_project_with_task(browser, project_name)
    browser.get(f"{ANNOTATIONLAB_URL}/#/project/{project_name}/tasks")
    wait_for_task_page_load(browser)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Task 1"))
    ).click()
    wait_for_labeling_page_load(browser)
    retry = 1
    while retry < 5:
        try:
            WebDriverWait(browser, DRIVER_TIMEOUT).until(
                EC.visibility_of_element_located(
                    (By.CLASS_NAME, "ls-submit-btn")
                )
            )
            break
        except Exception:
            browser.refresh()
            retry += 1

    element = browser.find_element_by_class_name("ls-submit-btn")
    browser.execute_script("arguments[0].click();", element)

    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "btn-submit-completion"))
    )

    element = browser.find_element_by_id("btn-submit-completion")
    browser.execute_script("arguments[0].click();", element)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (
                By.XPATH,
                '//div[@class="ant-modal-footer"]/button/span[contains(.,'
                ' "Submit")]',
            )
        )
    )
    assert (
        len(
            browser.find_elements_by_xpath(
                "//div[@class='ant-modal-footer']/button/span[contains(.,"
                " 'Submit')]"
            )
        )
        == 1
    )
    submit_button = browser.find_element_by_xpath(
        '//div[@class="ant-modal-footer"]/button/span[contains(., "Submit")]'
    )
    browser.execute_script("arguments[0].click();", submit_button)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.element_to_be_clickable((By.CLASS_NAME, "ant-collapse-content-box"))
    )
    completions_ts = browser.find_elements_by_xpath(
        '//div[@class="ant-collapse-content-box"]/li'
    )
    for completion in completions_ts:
        if "ID 1" in completion.text:
            browser.execute_script("arguments[0].click();", completion)
            WebDriverWait(completion, DRIVER_TIMEOUT).until(
                EC.visibility_of_element_located(
                    (By.CLASS_NAME, "anticon-star")
                )
            )
            assert (
                len(
                    browser.find_elements_by_class_name(
                        "ant-btn-background-ghost"
                    )
                )
                == 0
            )
            element = browser.find_element_by_class_name("anticon-copy")
            browser.execute_script("arguments[0].click();", element)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.text_to_be_present_in_element(
            (By.XPATH, '//div[@class="ant-collapse-content-box"]/li'), "ID 1002"
        )
    )
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located((By.ID, "btn-submit-completion"))
    )
    element = browser.find_element_by_id("btn-submit-completion")
    browser.execute_script("arguments[0].click();", element)
    WebDriverWait(browser, DRIVER_TIMEOUT).until(
        EC.visibility_of_element_located(
            (
                By.XPATH,
                '//div[@class="ant-modal-footer"]/button/span[contains(.,'
                ' "Submit")]',
            )
        )
    )
    assert (
        len(
            browser.find_elements_by_xpath(
                "//div[@class='ant-modal-footer']/button/span[contains(.,"
                " 'Submit')]"
            )
        )
        == 2
    )
    cancel_button = browser.find_element_by_xpath(
        '//div[@class="ant-modal-footer"]/button/span[contains(., "Cancel")]'
    )
    browser.execute_script("arguments[0].click();", cancel_button)
    delete_project(browser, project_name)
