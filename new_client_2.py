import sys
from datetime import timedelta, datetime
import json
import re
import requests
import getopt
import urllib3
import os
import time
import html
from jira import JIRA
from bs4 import BeautifulSoup
import markdown
import base64

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager(cert_reqs='CERT_NONE')
headers = {'Content-Type': 'application/json'}
# %%
# DBRKS URL pattern
pattern = r"^https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)$"
pattern_as_text = r"https://adb-([0-9]+).([0-9]+).azuredatabricks.net/\?o=([0-9]+)#job/([0-9]+)/run/([0-9]+)"
cleanRe = re.compile("<.*?>")

app_summary_map = {}
app_summary_map_list = []

events_map = {
    "efficiency": "Insights to make this app resource/cost efficient",
    "appFailure": "Insights to help with failure analysis",
    "Bottlenecks": "Insights to make this app faster",
    "SLA": "Insights to make this app meet SLA",
}

# Git specific variables
pr_number = os.getenv("PR_NUMBER")
repo_name = os.getenv("GITHUB_REPOSITORY")
# access_token = os.getenv("GITHUB_TOKEN")
access_token = os.getenv("GIT_TOKEN")
pr_url = os.getenv("PR_URL")
pr_user_email = os.getenv("PR_USER_EMAIL")
pr_commit_id = os.getenv("COMMIT_SHA")
pr_base_branch = os.getenv("BASE_BRANCH")
pr_target_branch = os.getenv("TARGET_BRANCH")

# Unravel specific variables
unravel_url = os.getenv("UNRAVEL_URL")
unravel_token = os.getenv("UNRAVEL_JWT_TOKEN")

# Slack specific variables
slack_webhook = os.getenv("SLACK_WEBHOOK")

# Jira specific variables
domain = os.getenv("JIRA_DOMAIN")
email = os.getenv("JIRA_EMAIL")
project_key = os.getenv("JIRA_PROJECT_KEY")
api_token = os.getenv("JIRA_API_TOKEN")

# if "https://" in unravel_url:
#     lr_url = unravel_url.replace("https://", "http://") + ":4043"
# else:
#     lr_url = unravel_url + ":4043"
lr_url = "http://18.204.206.1:4043"

file_code_map = {}
file_code_map['df.toPandas()'] = 'toPandas() moves all the data to driver to convert the spark df to a pandas dataframe.\n\n Instead use this statement df.withColumn("<newColumn>", lit("<constant_value>"))'
file_code_map['df.collect()'] = 'Avoid collecting all the data if we require only few rows of dataframe..\n\n Instead use this statement df.take(n)'



# %%
def get_api(api_url, api_token):
    response = requests.get(api_url, verify=False, headers={"Authorization": api_token})
    print(response.content)
    json_obj = json.loads(response.content)
    return json_obj


def check_response_on_get(json_val):
    if "message" in json_val:
        if json_val["message"] == "INVALID_TOKEN":
            raise ValueError("INVALID_TOKEN")


# %%
def search_summary_by_globalsearchpattern(
    base_url, api_token, start_time, end_time, gsp
):
    api_url = base_url + "/api/v1/ds/api/v1/databricks/runs/" + gsp + "/tasks/summary"
    print("URL: " + api_url)
    json_val = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val


# %%
def search_analysis(base_url, api_token, clusterUId, id):
    api_url = base_url + "/api/v1/spark/" + clusterUId + "/" + id + "/analysis"
    print("URL: " + api_url)
    json_val = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val


def search_summary(base_url, api_token, clusterUId, id):
    api_url = base_url + "/api/v1/spark/" + clusterUId + "/" + id + "/appsummary"
    print("URL: " + api_url)
    json_val = get_api(api_url, api_token)
    check_response_on_get(json_val)
    return json_val


# %%
def get_job_runs_from_description(pr_id, description_json):
    job_run_list = []
    for run_url in description_json["runs"]:
        match = re.search(pattern, run_url)
        if match:
            print(run_url)
            workspace_id = match.group(3)
            job_id = match.group(4)
            run_id = match.group(5)
            job_run_list.append(
                {
                    "pr_id": pr_id,
                    "pdbrks_url": run_url,
                    "workspace_id": workspace_id,
                    "job_id": job_id,
                    "run_id": run_id,
                }
            )

    return job_run_list


# %%
def get_job_runs_from_description_as_text(pr_id, description_text):
    job_run_list = []
    print("Description:\n" + description_text)
    print("Patten: " + pattern_as_text)
    matches = re.findall(pattern_as_text, description_text)
    if matches:
        for match in matches:
            workspace_id = match[2]
            job_id = match[3]
            run_id = match[4]
            job_run_list.append(
                {
                    "pr_id": pr_id,
                    "workspace_id": workspace_id,
                    "job_id": job_id,
                    "run_id": run_id,
                }
            )
    else:
        print("no match")
    return job_run_list


# %%
def get_organization_connection(organization_url, personal_access_token):
    credentials = BasicAuthentication("", personal_access_token)
    connection = Connection(base_url=organization_url, creds=credentials)
    return connection


def fetch_app_summary(unravel_url, unravel_token, clusterUId, appId):
    app_summary_map_for_git_comment = {}
    app_summary_map_for_jira_comments = {}
    autoscale_dict = {}
    summary_dict = search_summary(unravel_url, unravel_token, clusterUId, appId)
    summary_dict = summary_dict["annotation"]
    url = "{}/#/app/application/spark?execId={}&clusterUid={}".format(
        unravel_url, appId, clusterUId
    )
    app_summary_map_for_git_comment["Spark App"] = "[{}]({})".format(appId, url)
    app_summary_map_for_jira_comments["Spark App"] = "[{}|{}]".format(appId, url)
    cluster_url = "{}/#/compute/cluster_summary?cluster_uid={}&app_id={}".format(
        unravel_url, clusterUId, appId
    )
    app_summary_map_for_git_comment["Cluster"] = "[{}]({})".format(
        clusterUId, cluster_url
    )
    app_summary_map_for_jira_comments["Cluster"] = "[{}|{}]".format(
        clusterUId, cluster_url
    )

    estimated_cost = summary_dict["cents"] + summary_dict["dbuCost"]

    app_summary_map_for_git_comment["Estimated cost"] = "$ {}".format(
        round(estimated_cost, 3)
    )
    app_summary_map_for_jira_comments["Estimated cost"] = "$ {}".format(
        round(estimated_cost, 3)
    )
    runinfo = json.loads(summary_dict["runInfo"])
    app_summary_map_for_git_comment["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map_for_jira_comments["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map_for_git_comment["Driver Node Type"] = runinfo["driver_node_type_id"]
    app_summary_map_for_jira_comments["Driver Node Type"] = runinfo[
        "driver_node_type_id"
    ]
    app_summary_map_for_git_comment["Tags"] = runinfo["default_tags"]
    # tags = json.loads(runinfo["default_tags"])
    cluster_name = runinfo["default_tags"]['ClusterName']
    app_summary_map_for_jira_comments["Tags"] = runinfo["default_tags"]
    if "custom_tags" in runinfo.keys():
        app_summary_map_for_git_comment["Tags"] = {
            **app_summary_map_for_git_comment["Tags"],
            **runinfo["default_tags"],
        }
    if "autoscale" in runinfo.keys():
        autoscale_dict["autoscale_min_workers"] = runinfo["autoscale"]["min_workers"]
        autoscale_dict["autoscale_max_workers"] = runinfo["autoscale"]["max_workers"]
        autoscale_dict["autoscale_target_workers"] = runinfo["autoscale"][
            "target_workers"
        ]
        app_summary_map_for_git_comment["Autoscale"] = autoscale_dict
        app_summary_map_for_jira_comments["Autoscale"] = autoscale_dict
    else:
        app_summary_map_for_git_comment["Autoscale"] = "Autoscale is not enabled."
        app_summary_map_for_jira_comments["Autoscale"] = "Autoscale is not enabled."
    return app_summary_map_for_git_comment, app_summary_map_for_jira_comments, cluster_name


def get_pr_description():
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/{repo_name}/pulls/{pr_number}"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    pr_data = response.json()
    description = pr_data["body"]
    return description


def send_markdown_to_slack(channel, message):
    payload = {"channel": channel, "text": message, "mrkdwn": True}
    response = requests.post(slack_webhook, json=payload)
    if response.status_code == 200:
        print("Message sent successfully to Slack!")
    else:
        print(f"Failed to send message to Slack. Error: {response.text}")


def raise_jira_ticket(message):
    # Connect to Jira
    jira = JIRA(server="https://{}".format(domain), basic_auth=(email, api_token))

    # Create the issue
    issue_data = {
        "project": {"key": "CICD"},
        "summary": "Issue summary",
        "description": message,
        "issuetype": {"name": "Task"},
    }

    new_issue = jira.create_issue(fields=issue_data)

    print(new_issue)

    jira_link = "https://{}/browse/{}".format(domain, new_issue)

    return jira_link


def create_markdown_from_html(html_string):
    # Parse the HTML string
    soup = BeautifulSoup(html_string, "html.parser")

    # Find all <li> tags
    li_tags = soup.find_all("li")

    # Extract the text from each <li> tag
    bullet_points = [li.get_text(strip=True) for li in li_tags]

    # Convert bullet points to Markdown
    markdown_text = "\n".join([f"- {point}" for point in bullet_points])

    # Print the Markdown text
    print(markdown_text)

    return markdown_text


def get_pr_reviewers_list():
    # Set the GitHub access token

    # Set the API endpoint
    api_endpoint = f"https://api.github.com/repos/{repo_name}/pulls/{pr_number}"

    # Set the request headers
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Send the API request
    response = requests.get(api_endpoint, headers=headers)

    # Check the response status code
    if response.status_code == 200:
        # Parse the response JSON
        pr_data = response.json()

        # Get the list of reviewers
        reviewers = [reviewer["login"] for reviewer in pr_data["requested_reviewers"]]

        # Print the reviewers
        print("Reviewers:", reviewers)
        return reviewers
    else:
        print("Failed to fetch pull request details:", response.text)
        return []


def create_jira_message(job_run_result_list):
    comments = ""
    comments += "----\n"
    comments += "This Issue was automatically created by Unravel to follow up on the insights generated for the runs of the jobs mentioned in the description of pr number {} was raised to merge {} from {} to {}\n".format(
        pr_number, pr_commit_id, pr_base_branch, pr_target_branch
    )
    if job_run_result_list:
        for r in job_run_result_list:
            comments += "----\n"
            comments += "Workspace Id: {}, Job Id: {}, Run Id: {}\n\n".format(
                r["workspace_id"], r["job_id"], r["run_id"]
            )
            comments += "----\n"
            comments += "[{}|{}]\n".format("Unravel URL", r["unravel_url"])

            if r["jir_app_summary"]:
                headers = list(r["jir_app_summary"].keys())
                header_row = "|| " + " || ".join(headers) + " |\n"
                data_rows = (
                    "| "
                    + " | ".join(str(r["jir_app_summary"].get(h, "")) for h in headers)
                    + " |\n"
                )
                comments += "----\n"
                comments += "App Summary\n"
                comments += "*Estimated cost is the sum of DBUs and VM Cost\n"
                comments += "----\n"
                comments += "\n" + header_row + data_rows

            if r["unravel_insights"]:
                comments += "\nUnravel Insights\n"
                comments += "----\n"
                for insight in r["unravel_insights"]:
                    categories = insight["categories"]
                    if categories:
                        for k in categories.keys():
                            instances = categories[k]["instances"]
                            if instances:
                                for i in instances:
                                    if i["key"].upper() != "SPARKAPPTIMEREPORT":
                                        comments += "\n"
                                        comments += "|| {}: {} ||\n".format(
                                            i["key"].upper(), events_map[i["key"]]
                                        )
                                        comments += "| ℹ️ *{}* 	|\n".format(i["title"])
                                        comments += "| ⚡ *Details*\n{}	|\n".format(
                                            i["events"]
                                        )
                                        if "<li>" in i["actions"]:
                                            comments += "| 🛠 *Actions*\n{}	|\n".format(
                                                create_markdown_from_html(i["actions"])
                                            )
                                        else:
                                            comments += "| 🛠 *Actions*\n{}	|\n".format(
                                                i["actions"]
                                            )
                                        comments += "\n"
    return comments


def search_string_in_code(code, search_string):
    line_numbers = []
    lines = code.split("\n")
    for line_number, line in enumerate(lines, 1):
        if search_string in line:
            line_numbers.append(line_number)

    all_lines = []
    for line_number in line_numbers:
        result = []
        start_line = max(1, line_number - 3)
        end_line = min(len(lines), line_number)
        for i in range(start_line, end_line + 1):
            result.append(f"{i} {lines[i - 1]}")
        all_lines.append(result)
    return line_numbers, all_lines

def create_custom_code_block_and_add_pr_comment(code_block):
    comment = "\n```python\n"
    for code in code_block:
        comment += "{}\n".format(code)
    comment += "```\n\n"
    return comment

def perform_code_review(get_file_name_flag=False):
    # Get the changed file paths from the pull request event payload
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}/files'
    response = requests.get(url, headers=headers)
    files = response.json()
    changed_files = [file['filename'] for file in files]
    if get_file_name_flag == True:
        return changed_files
    print(changed_files)

    file_contents = {}
    for file in files:
        file_url = file['raw_url']
        file_response = requests.get(file_url)
        file_content = file_response.text
        file_contents[file['filename']] = file_content
    # Personal access token (replace with your own token)


    for file_name, file_content in file_contents.items():
        for pattern, optimal_value in file_code_map.items():
            line_numbers, code_lines = search_string_in_code(file_content, pattern)

            # API endpoint
            url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}/comments'

            # Request headers
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/vnd.github.v3+json',
                'X-GitHub-Api-Version': '2022-11-28'
            }

            for line_number, code_block in zip(line_numbers,code_lines):

                # Request body
                data = {
                    'body': optimal_value,
                    'path': file_name,
                    'commit_id': pr_commit_id,
                    'line': line_number
                }

                # Send POST request
                response = requests.post(url, headers=headers, data=json.dumps(data))

                # Check response status
                if response.status_code == 201:
                    print('Comment added successfully.')
                else:
                    comment = create_custom_code_block_and_add_pr_comment(code_block)
                    comment += "\n\n"
                    comment += optimal_value
                    url = f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"

                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Accept": "application/vnd.github.v3+json",
                    }
                    payload = {"body": "{}".format(comment)}
                    response = requests.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    
# d = '''{            "eventName": "CICDUsageEvent",
#                     "eventType": "CICD",
#                     "eventNumber": 4793,
#                     "eventTime": 1696896310332,
#                     "entityGroup": 2,
#                     "entityType": 2,
#                     "entityId": "8122943658466030_259557806380750_3341164-114",
#                     "user": "null",
#                     "queue": "null",
#                     "clusterName": "job-259557806380750-run-3341164",
#                     "clusterUid": "1013-000816-6ap9chzt",
#                     "staticRank": 20,
#                     "dynamicRank": 20.0,
#                     "title": "",
#                     "detail": "",
#                     "actions": ""
# }'''

def index_for_timestamp(prefix, ts):
    timestamp_seconds = ts / 1000
    ts = datetime.utcfromtimestamp(timestamp_seconds)
    if type(ts) == str:
        ts = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
    year = ts.strftime("%Y")
    month = ts.strftime("%m")
    day = ts.strftime("%d")
    d = datetime(int(year), int(month), int(day)).toordinal()
    sunday = str(datetime.fromordinal(d - (d % 7)))[8:10]
    saturday = str(datetime.fromordinal(d - (d % 7) + 6))[8:10]
    return prefix + year + month + sunday + "_" + saturday

def send_update_to_unravel(notification_sent,user_ids,jira_link,pr_url,pr_number,repo_name,es_document_list):
    detail_dict = {}
    detail_dict['agent'] = "GitHub"
    detail_dict['repository'] = repo_name
    detail_dict['prNumber'] = pr_number
    detail_dict['reviewers'] = user_ids
    detail_dict['prLink'] = pr_url
    detail_dict['fileDiff'] = perform_code_review(get_file_name_flag=True)
    detail_dict['imsLink'] = jira_link
    detail_dict['notificationSent'] = notification_sent
    for documents in es_document_list:
        documents['detail'] = str(detail_dict)
        id = documents['job']
        del documents['job']
        event_time = documents['eventTime']
        documents = json.dumps(documents)
        index = index_for_timestamp('ev-', event_time)
        body = f'{index} event {id} {5} {documents}'
        print(body)
        try:
            r = http.request('PUT', f'{lr_url}/logs/hl/hl/{id}/_bulkr', body=body, headers=headers)
            if r.status // 100 != 2:
                print(f'LR request failed: status={r.status} body={body} resp={r.data.decode()}')
            else:
                print("request sent to LR")
        except Exception as err:
            print(f'LR request failed: body={body} error={err}')



def create_es_document(gsp, cluster_name, cluster_uid, job):
    document = {}
    document['eventName'] = "CICDUsageEvent"
    document['eventType'] = "CICD"
    document['eventNumber'] = pr_number
    document['eventTime'] = int(time.time() * 1000)
    document['entityGroup'] = 2
    document['entityType'] = 2
    document['entityId'] = gsp
    document['user'] = "null"
    document['queue'] = "null"
    document['clusterName'] = cluster_name
    document['clusterUid'] = cluster_uid
    document['staticRank'] = 2
    document['dynamicRank'] = 2
    document['title'] = "CICD Document"
    document['detail'] = "detail_dict"
    document['actions'] = "null"
    document['job'] = job
    return document




def create_comments_with_markdown(mk_list):
    comments = ""
    for mk in mk_list:
        if mk['key'] == "header":
            comments += "\n----\n"
            comments += mk['mk']
            comments += "\n"
    for mk in mk_list:
        if mk['key'] != "header":
            comments += "\n----\n"
            comments += "<details>\n"
            comments += "<summary> <h2><b>{}</b></h2></summary>\n\n".format(mk['key'])
            comments += "\n"
            comments += mk['mk']
            comments += "\n"
            comments += "</details>\n\n"
    return comments

def assign_reviewer():
    url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}/requested_reviewers'
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }
    
    data = {
        'reviewers': ["Unravel-Assistant"]
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code == 201:
        print('Reviewer added successfully.')
    else:
        print('Failed to add reviewer:', response.json())

def approve_pr():
    url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}/reviews'

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }
    
    data = {
        'body': 'Approving this PR as there are no further pending rewrites. Please take a look at other insights and consider their suggested resolutions to further optimize this job.',
        'event': 'APPROVE',
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code == 200:
        print('Pull request approved successfully.')
    else:
        print('Failed to approve pull request:', response.json())


def approve_review_comment():
    # API URL to get review comments for the PR
    url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}/comments'
    
    # Request headers
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }
    
    # Fetch all review comments for the PR
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        comments = response.json()
        if comments:
            for comment in comments:
                if comment['original_line'] == 47:
                    comment_id = comment['id']
                    print(comment)
                    update_url = f'https://api.github.com/repos/{repo_name}/pulls/comments/{comment_id}'
        
                    # Data for updating the comment
                    data = {
                        'body': "This issue has been resolved. The suggestion has been implemented. ✅"
                    }
        
                    # Send PATCH request to update the comment
                    update_response = requests.patch(update_url, headers=headers, data=json.dumps(data))
        
                    if update_response.status_code == 200:
                        print(f"Comment {comment_id} updated successfully.")
                    else:
                        print(f"Failed to update comment {comment_id}. Status code: {update_response.status_code}")
                        print(update_response.json())
        else:
            print("No comments found.")
    else:
        print(f"Failed to retrieve comments. Status code: {response.status_code}")
        print(response.json())


# %%
def main():
    raw_description = get_pr_description()
    es_document_list = []
    desc = True
    if not raw_description:
        desc = False
    else:
        description = " ".join(raw_description.splitlines())
        description = re.sub(cleanRe, "", description)
        job_run_list = get_job_runs_from_description_as_text(pr_number, description)
        print(job_run_list)
    mk_list = []
    # if job_run_list[0]['run_id'] != "20934596855561":
    #     approve_review_comment()
    #     approve_pr()
    if desc:
        assign_reviewer()
        value = """
| Spark App               | Cluster              | Estimated Cost | Executor Node Type | Driver Node Type   | Tags                                                                                                                                                                                                                            | Autoscale                 |
|-------------------------|----------------------|----------------|--------------------|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| app-20241208114502-0000 | 1208-114055-sivg3fp6 | $0.1           | Standard_DS3_v2    | Standard_DS3_v2    | {'Vendor': 'Databricks', 'Creator': 'ptholeti@unraveldata.com', 'ClusterName': 'job-263734785050587-run-2549400', 'ClusterId': '0607-080548-qf2z9hbu', 'JobId': '263734785050587', 'RunName': 'Multiple Jobs Stages Simulator'} | Autoscale is not enabled. |
"""
        mk_list.append({"key":"header", "mk": value})
        value = """
## Insight: Node Resizing for Jobs Compute

### Problem:

# Problem Statement

**Job:** `Inefficient_JoinType_EventGenerator_Application`  
**Workspace:** `dbx.v4796.playground.customer-demos.shwetha`  
**Cluster:** `1208-114055-sivg3fp6`  
**Spark Application ID:** `app-20241208114502-0000`  
**Recent Run ID:** `149641318783677`  

---

## Issue

The job is experiencing **over-provisioning of resources** for the allocated cluster, leading to underutilization and increased costs.

### Resource Over-Provisioning Details

| Component  | Cores (vCPUs) | Memory (GB) |
|------------|---------------|-------------|
| **Driver** | 4             | 14          |
| **Worker** | 4             | 14          |

---

## Observations

1. **Excess Resource Allocation**  
   - Both the driver and worker nodes are over-provisioned for the workload, resulting in resource underutilization.

2. **Low Cost Impact**  
   - Although the potential savings are modest (**$0.06/month**, **$0.73/year**), optimizing the configuration can enhance cost efficiency for larger-scale or similar workloads.

---

## Recommendations

1. **Right-Size the Cluster**
   - Reduce the cores and memory allocated to the driver and workers to match actual workload requirements.  

2. **Enable Autoscaling**
   - Configure autoscaling to dynamically adjust resource allocation based on workload demands, especially for bursty or unpredictable jobs.  

3. **Analyze Workload Usage**
   - Use monitoring tools to understand resource consumption trends and adjust configurations accordingly.

4. **Consider Shared Resources**
   - For jobs with minimal cost impact, consider running them on shared or smaller clusters to reduce overhead.

---

## Potential Savings

- **Monthly Savings:** $0.06  
- **Annualized Savings:** $0.73  

While the immediate savings are small, implementing these changes ensures better scalability and cost management for future workloads.


### Remediation:
Go to the cluster configuration page for the job [**257510190261732**](https://adb-1635409050664771.11.azuredatabricks.net/?o=1635409050664771#job/257510190261732) and change the driver node to **Standard_L4s** and worker node to **Standard_L4s** respectively to maximize cost savings.    
    

&nbsp;     
<!-- Add your HTML table here -->
<div><div class="unravel-card shadow"><div class="v2 unravel-table-container compact-table p-0"><div class="table-title"><h5>Driver node recommendations</h5><div class="mt-2" style="color:var(--neutral-120)">Based on data from 2024-11-28 11:49:32 UTC to 2024-12-08 11:49:32 UTC.</div></div><table><thead><tr><th class="text-left text-ellipsis"></th><th class="text-left text-ellipsis">Current instance</th><th class="text-left text-ellipsis">Option 1  <span style="background-color:#486AE3;color:var(--neutral-10);padding:4px;border-radius:4px;font-weight:normal;font-size:10px">Max savings  </span></th><th class="text-left text-ellipsis">Option 2</th><th class="text-left text-ellipsis">Option 3</th></tr></thead><tbody><tr><td class="text-ellipsis">Compute</td><td class="text-ellipsis" style="color:var(--neutral-120)">Standard_DS3_v2</td><td class="text-ellipsis">Standard_L4s</td><td class="text-ellipsis">Standard_F4</td><td class="text-ellipsis">Standard_F4s_v2</td></tr><tr><td class="text-ellipsis">Memory</td><td class="text-ellipsis" style="color:var(--neutral-120)">14 GiB</td><td class="text-ellipsis">32 GiB</td><td class="text-ellipsis">8 GiB</td><td class="text-ellipsis">8 GiB</td></tr><tr><td class="text-ellipsis">Cores</td><td class="text-ellipsis" style="color:var(--neutral-120)">4 cores</td><td class="text-ellipsis">4 cores</td><td class="text-ellipsis">4 cores</td><td class="text-ellipsis">4 cores</td></tr><tr><td class="text-ellipsis">Price per hour</td><td class="text-ellipsis" style="color:var(--neutral-120)">$ 0.41 /hr</td><td class="text-ellipsis">$ 0.15 /hr</td><td class="text-ellipsis">$ 0.27 /hr</td><td class="text-ellipsis">$ 0.28 /hr</td></tr><tr><td>Type</td><td style="color:var(--neutral-120)">General purpose</td><td>Storage optimized</td><td>Compute optimized</td><td>Compute optimized</td></tr><tr><td class="text-ellipsis">Potential savings per run</td><td class="text-ellipsis" style="color:var(--neutral-120)"></td><td class="text-ellipsis">$ 0.0201 (20.78 %)</td><td class="text-ellipsis">$ 0.0102 (10.56 %)</td><td class="text-ellipsis">$ 0.0096 (9.90 %)</td></tr></tbody></table></div></div><div class="unravel-card shadow"><div class="v2 unravel-table-container compact-table p-0"><div class="table-title"><h5>Worker node recommendations</h5><div class="mt-2" style="color:var(--neutral-120)">Based on data from 2024-11-28 11:49:32 UTC to 2024-12-08 11:49:32 UTC.</div></div><table><thead><tr><th class="text-left text-ellipsis"></th><th class="text-left text-ellipsis">Current instance</th><th class="text-left text-ellipsis">Option 1  <span style="background-color:#486AE3;color:var(--neutral-10);padding:4px;border-radius:4px;font-weight:normal;font-size:10px">Max savings  </span></th><th class="text-left text-ellipsis">Option 2</th><th class="text-left text-ellipsis">Option 3</th></tr></thead><tbody><tr><td class="text-ellipsis">Compute</td><td class="text-ellipsis" style="color:var(--neutral-120)">Standard_DS3_v2</td><td class="text-ellipsis">Standard_L4s</td><td class="text-ellipsis">Standard_F4s</td><td class="text-ellipsis">Standard_F4</td></tr><tr><td class="text-ellipsis">Memory</td><td class="text-ellipsis" style="color:var(--neutral-120)">14.0 GiB</td><td class="text-ellipsis">32 GiB</td><td class="text-ellipsis">8 GiB</td><td class="text-ellipsis">8 GiB</td></tr><tr><td class="text-ellipsis">Cores</td><td class="text-ellipsis" style="color:var(--neutral-120)">4 cores</td><td class="text-ellipsis">4 cores</td><td class="text-ellipsis">4 cores</td><td class="text-ellipsis">4 cores</td></tr><tr><td class="text-ellipsis">Price per hour</td><td class="text-ellipsis" style="color:var(--neutral-120)">$ 0.41 /hr</td><td class="text-ellipsis">$ 0.15 /hr</td><td class="text-ellipsis">$ 0.27 /hr</td><td class="text-ellipsis">$ 0.27 /hr</td></tr><tr><td>Type</td><td style="color:var(--neutral-120)">General purpose</td><td>Storage optimized</td><td>Compute optimized</td><td>Compute optimized</td></tr><tr><td class="text-ellipsis">Potential savings per run</td><td class="text-ellipsis" style="color:var(--neutral-120)"></td><td class="text-ellipsis">$ 0.0402 (41.57 %)</td><td class="text-ellipsis">$ 0.0204 (21.12 %)</td><td class="text-ellipsis">$ 0.0204 (21.12 %)</td></tr></tbody></table></div></div></div>


"""
        mk_list.append({"key":"Cost Savings Insights", "mk": value})
        
        body_text = """
### Inefficient join type

# Problem

# Problem Statement

**Job:** `Inefficient_JoinType_EventGenerator_Application`  
**Workspace:** `dbx.v4796.playground.customer-demos.shwetha`  
**Cluster:** `1208-114055-sivg3fp6`  
**Spark Application ID:** `app-20241208114502-0000`  
**Recent Run ID:** `149641318783677`  

---

## Issue

The job is experiencing **over-provisioning of resources** for the allocated cluster, leading to underutilization and increased costs.

### Resource Over-Provisioning Details

| Component  | Cores (vCPUs) | Memory (GB) |
|------------|---------------|-------------|
| **Driver** | 4             | 14          |
| **Worker** | 4             | 14          |

---

## Observations

1. **Excess Resource Allocation**  
   - Both the driver and worker nodes are over-provisioned for the workload, resulting in resource underutilization.

2. **Low Cost Impact**  
   - Although the potential savings are modest (**$0.06/month**, **$0.73/year**), optimizing the configuration can enhance cost efficiency for larger-scale or similar workloads.

---

## Recommendations

1. **Right-Size the Cluster**
   - Reduce the cores and memory allocated to the driver and workers to match actual workload requirements.  

2. **Enable Autoscaling**
   - Configure autoscaling to dynamically adjust resource allocation based on workload demands, especially for bursty or unpredictable jobs.  

3. **Analyze Workload Usage**
   - Use monitoring tools to understand resource consumption trends and adjust configurations accordingly.

4. **Consider Shared Resources**
   - For jobs with minimal cost impact, consider running them on shared or smaller clusters to reduce overhead.

---

## Potential Savings

- **Monthly Savings:** $0.06  
- **Annualized Savings:** $0.73  

While the immediate savings are small, implementing these changes ensures better scalability and cost management for future workloads.


## Recommendations


Use broadcast hints to broadcast the smaller table.    

Broadcast hints in Databricks enable the forced broadcast of small tables to all worker nodes, optimizing join operations by reducing shuffling and network overhead.          
This is especially useful when joining a large dataset with a smaller one, ensuring faster and more efficient query performance.     

"""
        mk_list.append({"key":"Inefficient join condition", "mk": body_text})
    else:
        assign_reviewer()
        value = """
| Spark App               | Cluster              | Estimated Cost | Executor Node Type | Driver Node Type   | Tags                                                                                                                                                                                                                            | Autoscale                 |
|-------------------------|----------------------|----------------|--------------------|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| app-20241205115301-0000 | 1205-114957-63lf6ph3 | $9.51          | Standard_D16ads_v5 | Standard_D16ads_v5 | {'Vendor': 'Databricks', 'Creator': 'ptholeti@unraveldata.com', 'ClusterName': 'job-263734785050587-run-2549400', 'ClusterId': '0607-080548-qf2z9hbu', 'JobId': '263734785050587', 'RunName': 'Multiple Jobs Stages Simulator'} | Autoscale is not enabled. |
"""
        mk_list.append({"key":"header", "mk": value})
        value = """
## Insight: Node Resizing for Jobs Compute

### Problem:

**Job:** `NodeRightSizeWithAutoScale_2`  
**Workspace:** `dbx.v4796.playground.customer-demos.shwetha`  
**Issue:** Contention with driver time comprising **61% of total execution time**, indicating possible inefficiencies in the code or bottlenecks on resources.  

**Recent Run ID:** `20934596855561`

---

## Observations

1. **Driver-Only Operations**  
   - The majority of operations are performed on the driver (e.g., Python pandas processing).  
   - Heavy operations on the driver lead to **underutilized executors**, impacting both **cost** and **performance**.  

2. **Idle Cluster**  
   - The cluster remains idle for extended periods, likely due to a **long auto-termination timeout**.  

3. **External Dependency Delays**  
   - Driver code is waiting for **slow responses from external services or infrastructure** (e.g., SQL database reads).  

---

## Potential Savings

- **Monthly Savings:** $8.17  
- **Annualized Savings:** $99.43  

---

## Next Steps

To realize the savings, review the following **remediation** actions:

- Optimize driver-based operations by delegating tasks to executors.  
- Reduce auto-termination timeout to minimize idle cluster costs.  
- Improve response times for external services or cache frequently accessed data.  

### Impact:
- This problem has affected 43/137 (31%) runs.
- Applying the recommended insights could have resulted in untapped savings of up to $1.28 in the last 10 days, and $46.08 annualized.
- Diagnosing this issue has saved $221 (22 hours) in troubleshooting efforts.

### Remediation:
Go to the cluster configuration page for the job [**238605012616661**](https://adb-1635409050664771.11.azuredatabricks.net/?o=1635409050664771#job/238605012616661) and change the driver node to **Standard_L8s** and worker node to **Standard_D16ds_v4** respectively to maximize cost savings.    
Ensure autoscaling is enabled, set min workers to **2** and max workers to **5**    

&nbsp;     
<!-- Add your HTML table here -->
<div><div class="unravel-card shadow"><div class="v2 unravel-table-container compact-table p-0"><div class="table-title"><h5>Driver node recommendations</h5><div class="mt-2" style="color:var(--neutral-120)">Based on data from 2024-11-25 13:06:10 UTC to 2024-12-05 13:06:10 UTC.</div></div><table><thead><tr><th class="text-left text-ellipsis"></th><th class="text-left text-ellipsis">Current instance</th><th class="text-left text-ellipsis">Option 1  <span style="background-color:#486AE3;color:var(--neutral-10);padding:4px;border-radius:4px;font-weight:normal;font-size:10px">Max savings  </span></th><th class="text-left text-ellipsis">Option 2</th><th class="text-left text-ellipsis">Option 3</th></tr></thead><tbody><tr><td class="text-ellipsis">Compute</td><td class="text-ellipsis" style="color:var(--neutral-120)">Standard_D16ads_v5</td><td class="text-ellipsis">Standard_L8s</td><td class="text-ellipsis">Standard_D8d_v4</td><td class="text-ellipsis">Standard_D8ads_v5</td></tr><tr><td class="text-ellipsis">Memory</td><td class="text-ellipsis" style="color:var(--neutral-120)">64 GiB</td><td class="text-ellipsis">64 GiB</td><td class="text-ellipsis">32 GiB</td><td class="text-ellipsis">32 GiB</td></tr><tr><td class="text-ellipsis">Cores</td><td class="text-ellipsis" style="color:var(--neutral-120)">16 cores</td><td class="text-ellipsis">8 cores</td><td class="text-ellipsis">8 cores</td><td class="text-ellipsis">8 cores</td></tr><tr><td class="text-ellipsis">Price per hour</td><td class="text-ellipsis" style="color:var(--neutral-120)">$ 1.42 /hr</td><td class="text-ellipsis">$ 0.30 /hr</td><td class="text-ellipsis">$ 0.68 /hr</td><td class="text-ellipsis">$ 0.71 /hr</td></tr><tr><td>Type</td><td style="color:var(--neutral-120)">General purpose</td><td>Storage optimized</td><td>General purpose</td><td>General purpose</td></tr><tr><td class="text-ellipsis">Potential savings per run</td><td class="text-ellipsis" style="color:var(--neutral-120)"></td><td class="text-ellipsis">$ 1.1868 (12.48 %)</td><td class="text-ellipsis">$ 0.7912 (8.32 %)</td><td class="text-ellipsis">$ 0.7607 (8.00 %)</td></tr></tbody></table></div></div><div class="unravel-card shadow"><div class="v2 unravel-table-container compact-table p-0"><div class="table-title"><h5>Worker node recommendations</h5><div class="mt-2" style="color:var(--neutral-120)">Based on data from 2024-11-25 13:06:10 UTC to 2024-12-05 13:06:10 UTC.</div></div><table><thead><tr><th class="text-left text-ellipsis"></th><th class="text-left text-ellipsis">Current instance</th><th class="text-left text-ellipsis">Option 1  <span style="background-color:#486AE3;color:var(--neutral-10);padding:4px;border-radius:4px;font-weight:normal;font-size:10px">Max savings  </span></th><th class="text-left text-ellipsis">Option 2</th><th class="text-left text-ellipsis">Option 3</th></tr></thead><tbody><tr><td class="text-ellipsis">Compute</td><td class="text-ellipsis" style="color:var(--neutral-120)">Standard_D16ads_v5</td><td class="text-ellipsis">Standard_D16ds_v4</td><td class="text-ellipsis">Standard_D16d_v4</td><td class="text-ellipsis">Standard_D16a_v4</td></tr><tr><td class="text-ellipsis">Memory</td><td class="text-ellipsis" style="color:var(--neutral-120)">64.0 GiB</td><td class="text-ellipsis">64 GiB</td><td class="text-ellipsis">64 GiB</td><td class="text-ellipsis">64 GiB</td></tr><tr><td class="text-ellipsis">Cores</td><td class="text-ellipsis" style="color:var(--neutral-120)">16 cores</td><td class="text-ellipsis">16 cores</td><td class="text-ellipsis">16 cores</td><td class="text-ellipsis">16 cores</td></tr><tr><td class="text-ellipsis">Price per hour</td><td class="text-ellipsis" style="color:var(--neutral-120)">$ 1.42 /hr</td><td class="text-ellipsis">$ 1.35 /hr</td><td class="text-ellipsis">$ 1.35 /hr</td><td class="text-ellipsis">$ 1.22 /hr</td></tr><tr><td>Type</td><td style="color:var(--neutral-120)">General purpose</td><td>General purpose</td><td>General purpose</td><td>General purpose</td></tr><tr><td class="text-ellipsis">Min Worker</td><td class="text-ellipsis" style="color:var(--neutral-120)"></td><td class="text-ellipsis">2</td><td class="text-ellipsis">2</td><td class="text-ellipsis">2</td></tr><tr><td class="text-ellipsis">Max Worker</td><td class="text-ellipsis" style="color:var(--neutral-120)"></td><td class="text-ellipsis">5</td><td class="text-ellipsis">5</td><td class="text-ellipsis">5</td></tr><tr><td class="text-ellipsis">Potential savings per run</td><td class="text-ellipsis" style="color:var(--neutral-120)"></td><td class="text-ellipsis">$ 6.9871 (73.47 %)</td><td class="text-ellipsis">$ 6.9871 (73.47 %)</td><td class="text-ellipsis">$ 6.3872 (67.16 %)</td></tr></tbody></table></div></div></div>

"""
        mk_list.append({"key":"Cost Savings Insights", "mk": value})
        body_text = """
### Contended Driver

# Problem

**Job:** `NodeRightSizeWithAutoScale_2`  
**Workspace:** `dbx.v4796.playground.customer-demos.shwetha`  
**Issue:** Contention with driver time comprising **61% of total execution time**, indicating possible inefficiencies in the code or bottlenecks on resources.  

**Recent Run ID:** `20934596855561`

---

## Observations

1. **Driver-Only Operations**  
   - The majority of operations are performed on the driver (e.g., Python pandas processing).  
   - Heavy operations on the driver lead to **underutilized executors**, impacting both **cost** and **performance**.  

2. **Idle Cluster**  
   - The cluster remains idle for extended periods, likely due to a **long auto-termination timeout**.  

3. **External Dependency Delays**  
   - Driver code is waiting for **slow responses from external services or infrastructure** (e.g., SQL database reads).  

---

## Potential Savings

- **Monthly Savings:** $8.17  
- **Annualized Savings:** $99.43  

---

## Next Steps

To realize the savings, review the following **remediation** actions:

- Optimize driver-based operations by delegating tasks to executors.  
- Reduce auto-termination timeout to minimize idle cluster costs.  
- Improve response times for external services or cache frequently accessed data.  


**Remediation**

Remediation steps will vary based on the identified root cause. Common actions include:      


#### Avoid heavy Python operations in driver code       
If the driver code involves heavy Python operations like pandas processing after data collection, consider rewriting the code to leverage Spark for processing directly on the workers.
- Replace `toPandas()` with Spark distributed DataFrames using `pandas_api()` to avoid collecting all data at the driver.  
- Ensure pandas DataFrame operations are avoided in PySpark code; instead, leverage `pandas_api()` functions.  
- If the workers are sitting idle due to a long auto-termination timeout, consider lowering the timeout interval.


#### Optimize for slow external services    
If the driver code is waiting on external services or infrastructure that are slow to respond (e.g., waiting on reads from an external SQL database), consider increasing the number of partitions to work in parallel or using a separate Databricks Job to copy the data asynchronously ahead of time.


#### Avoid collecting results at driver   
If the driver code is collecting the results, try to avoid collecting all records and instead collect only a few samples.  
For example: Replace `collect()` with functions like `take(n)`.

"""
        mk_list.append({"key":"Contended Driver", "mk": body_text})
        body_text = """
### Inefficient join condition

# Problem

**Job:** `NodeRightSizeWithAutoScale_2`  
**Workspace:** `dbx.v4796.playground.customer-demos.shwetha`  
**Stage:** 10  
**Spark Application ID:** `app-20241205115301-0000`  
**Recent Run ID:** `20934596855561`  

---

## Issue

The **BroadcastHashJoin** operator in stage 10 has an **inefficient join condition**, leading to performance bottlenecks:

1. **Symptoms:**  
   - The total number of joined rows exceeds the maximum number of rows in either the left or right join tables.  
   - This indicates that the join keys may have **low cardinality**, resulting in redundant rows being generated.

2. **Implications:**  
   - Increased computational overhead.  
   - Potential underutilization of resources.  

---

## Recommendations

Verify if there are a lot of null values in tables used in the join or group-by keys. If yes, preprocess the null values before proceeding with the solution if necessary.    

#### Re-partition the data     

- Review the join conditions and keys in your query. Check for any skewness (an uneven distribution of data) in the tables involved in the join condition. 
- If skewness is found, consider repartitioning the data before the join.

#### Salting Technique   

- Consider altering the join key to ensure even distribution of data using the salting method. For instance, if data skew is caused by a specific column (key), adding a random partitioning key can help to evenly distribute the data as follows:

```scala
val modifiedKeyForData = origDataSkew.map { case (key, value) => 
  (key + scala.util.Random.nextInt(currentPartitions), value) 
}
val dataSkewFixed = modifiedKeyForData.partitionBy(currentPartitions)
```

### Best Practices     
- It typically occurs due to uneven key distributions or changes in data characteristics in dynamic workloads.
- This issue happens when the join predicate is either missed/omitted (i.e., a Cartesian Product) or the join predicates are not selective enough.
- In such cases, the developer should review the query to see if the exploding join is warranted by business semantics or can be improved by adding more selective join predicates.

"""
        mk_list.append({"key":"Inefficient join condition", "mk": body_text})

    if True:
        time.sleep(5)
        # unravel_comments = re.sub(cleanRe, '', json.dumps(job_run_result_list, indent=4))
        unravel_comments = create_comments_with_markdown(mk_list)

        url = f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        payload = {"body": "{}".format(unravel_comments)}
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        channel = "#cicd-notifications"
        # Replace with your Markdown-formatted message
        message = "Unravel has insights for the pr number {} which was raised to merge {} from {} to {}. Click this link for further details {}".format(
            pr_number, pr_commit_id, pr_base_branch, pr_target_branch, pr_url
        )
        # Format the user IDs with '@' symbol
        user_ids = get_pr_reviewers_list()
        formatted_user_ids = ["@" + user_id for user_id in user_ids]

        # Create the message text with user mentions
        message_with_mentions = message + " " + " ".join(formatted_user_ids)

        try:
            # send_markdown_to_slack(channel, message_with_mentions)
            notification_sent = True
        except:
            notification_sent = False

    else:
        print("Nothing to do without Unravel integration")
        sys.exit(0)


# %%
if __name__ == "__main__":
    main()
