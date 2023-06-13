import sys
from datetime import timedelta, datetime
import json
import re
import requests
import getopt
import urllib3
import os
import html
from html2jirawiki import html_to_jira_wiki

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
pr_number = os.getenv('PR_NUMBER')
repo_name = os.getenv('GITHUB_REPOSITORY')
access_token = os.getenv('GITHUB_TOKEN')
pr_url = os.getenv('PR_URL')
pr_user_email = os.getenv('PR_USER_EMAIL')
pr_commit_id = os.getenv('COMMIT_SHA')
pr_base_branch = os.getenv('BASE_BRANCH')
pr_target_branch = os.getenv('TARGET_BRANCH')

# Unravel specific variables
unravel_url = os.getenv('UNRAVEL_URL')
unravel_token = os.getenv('UNRAVEL_JWT_TOKEN')

# Slack specific variables
slack_webhook = os.getenv('SLACK_WEBHOOK')

# Jira specific variables
domain = os.getenv('JIRA_DOMAIN')
email = os.getenv('JIRA_EMAIL')
project_key = os.getenv('JIRA_PROJECT_KEY')
api_token = os.getenv('JIRA_API_TOKEN')


# %%
def get_api(api_url, api_token):
    response = requests.get(api_url, verify=False, headers={"Authorization": api_token})
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


# %%
def create_comments_with_markdown(job_run_result_list):
    comments = ""
    if job_run_result_list:
        for r in job_run_result_list:
            comments += "----\n"
            comments += "<details>\n"
            # comments += "<img src='https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg' alt='Logo'>\n\n"
            comments += "<summary> <img src='https://www.unraveldata.com/wp-content/themes/unravel-child/src/images/unLogo.svg' alt='Logo'> <b>Workspace Id: {}, Job Id: {}, Run Id: {}</b></summary>\n\n".format(
                r["workspace_id"], r["job_id"], r["run_id"]
            )
            comments += "----\n"
            comments += "#### [{}]({})\n".format('Unravel url', r["unravel_url"])
            if r['app_summary']:
                # Get all unique keys from the dictionaries while preserving the order
                headers = []
                for key in r['app_summary'].keys():
                    if key not in headers:
                        headers.append(key)

                # Generate the header row
                header_row = "| " + " | ".join(headers) + " |"

                # Generate the separator row
                separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

                # Generate the data rows
                data_rows = "\n".join(
                    [
                        "| " + " | ".join(str(r['app_summary'].get(h, "")) for h in headers)
                    ]
                )

                # Combine the header, separator, and data rows
                comments += "----\n"
                comments += "App Summary  <sup>*Estimated cost is sum of DBUs and VM Cost</sup>\n"
                comments += "----\n"
                comments += header_row + "\n" + separator_row + "\n" + data_rows + "\n"
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
                                        comments += "| {}: {} |\n".format(i["key"].upper(), events_map[i['key']])
                                        comments += "|---	|\n"
                                        comments += "| ℹ️ **{}** 	|\n".format(i['title'])
                                        comments += "| ⚡ **Details**<br>{}	|\n".format(i["events"])
                                        comments += "| 🛠 **Actions**<br>{}	|\n".format(i['actions'])
                                        comments += "\n"
            comments += "</details>\n\n"

    return comments


def fetch_app_summary(unravel_url, unravel_token, clusterUId, appId):
    app_summary_map = {}
    autoscale_dict = {}
    summary_dict = search_summary(unravel_url, unravel_token, clusterUId, appId)
    summary_dict = summary_dict["annotation"]
    url = '{}/#/app/application/spark?execId={}&clusterUid={}'.format(unravel_url, appId, clusterUId)
    app_summary_map["Spark App"] = '[{}]({})'.format(appId, url)
    cluster_url = '{}/#/compute/cluster_summary?cluster_uid={}&app_id={}'.format(unravel_url, clusterUId, appId)
    app_summary_map["Cluster"] = '[{}]({})'.format(clusterUId, cluster_url)
    app_summary_map["Estimated cost"] = '$ {}'.format(summary_dict["cents"] + summary_dict["dbuCost"])
    runinfo = json.loads(summary_dict["runInfo"])
    app_summary_map["Executor Node Type"] = runinfo["node_type_id"]
    app_summary_map["Driver Node Type"] = runinfo["driver_node_type_id"]
    app_summary_map["Tags"] = runinfo["default_tags"]
    if 'custom_tags' in runinfo.keys():
        app_summary_map["Tags"] = {**app_summary_map["Tags"], **runinfo["default_tags"]}
    if "autoscale" in runinfo.keys():
        autoscale_dict["autoscale_min_workers"] = runinfo["autoscale"]["min_workers"]
        autoscale_dict["autoscale_max_workers"] = runinfo["autoscale"]["max_workers"]
        autoscale_dict["autoscale_target_workers"] = runinfo["autoscale"][
            "target_workers"
        ]
        app_summary_map['Autoscale'] = autoscale_dict
    else:
        app_summary_map['Autoscale'] = 'Autoscale is not enabled.'
    return app_summary_map


def get_pr_description():
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json'
    }

    url = f'https://api.github.com/repos/{repo_name}/pulls/{pr_number}'

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    pr_data = response.json()
    description = pr_data['body']
    return description


def send_markdown_to_slack(channel, message):
    payload = {
        'channel': channel,
        'text': message,
        'mrkdwn': True
    }
    response = requests.post(slack_webhook, json=payload)
    if response.status_code == 200:
        print("Message sent successfully to Slack!")
    else:
        print(f"Failed to send message to Slack. Error: {response.text}")


def raise_jira_ticket(message):
    # API endpoint for creating an issue
    url = f'https://{domain}/rest/api/3/issue'

    # Headers and authentication
    headers = {
        'Content-Type': 'application/json'
    }
    auth = (email, api_token)

    # Issue data
    issue_data = {
        'fields': {
            'project': {
                'key': project_key
            },
            'summary': 'Issue summary',
            'description': {
                'type': 'doc',
                'version': 1,
                'content': [
                    {
                        'type': 'paragraph',
                        'content': [
                            {
                                'type': 'text',
                                'text': message
                            }
                        ]
                    }
                ]
            },
            'issuetype': {
                'name': 'Task'
            }
        }
    }

    # Create the issue
    response = requests.post(url, headers=headers, auth=auth, data=json.dumps(issue_data))

    # Check the response
    if response.status_code == 201:
        print('Issue created successfully!')
        issue_key = response.json()['key']
        print(f'Issue key: {issue_key}')
    else:
        print('Failed to create issue. Response:', response.text)


# def create_jira_message(job_run_result_list, link):
#     pattern = r'\((.*?)\)'
#     comment = "This Issue was automatically created by Unravel to follow up on the insights generated for the runs of the jobs mentioned in the description of pr number {} was raised to merge {} from {} to {} \n".format(pr_number,pr_commit_id,pr_base_branch,pr_target_branch)
#     if job_run_result_list:
#         for r in job_run_result_list:
#             comment += "\nDetails of the dbx job {}\n\n".format(r["job_id"])
#             comment += "Job ID: {}\n".format(r["unravel_url"])
#             comment += "Cluster ID: {}\n".format(re.search(pattern, r['app_summary']['Cluster']).group(1))
#             comment += "Spark App: {}\n".format(re.search(pattern, r['app_summary']['Spark App']).group(1))
#             comment += "Estimated Cost: {}\n".format(r['app_summary']['Estimated cost'])
#             comment += "Tags: {}\n".format(r['app_summary']['Tags'])
#             comment += "AutoScaling Info: {}\n".format(r['app_summary']['Autoscale'])
#             comment += "\n"
#             comment += "The following insights were generated\n\n"
#             if r["unravel_insights"]:
#                 for insight in r["unravel_insights"]:
#                     categories = insight["categories"]
#                     if categories:
#                         for k in categories.keys():
#                             instances = categories[k]["instances"]
#                             if instances:
#                                 for i in instances:
#                                     if i["key"].upper() != "SPARKAPPTIMEREPORT":
#                                         comment += "{}: {} \n".format(i["key"].upper(), events_map[i['key']])
#     comment += '\n\n For more detailed information clink this link {}'.format(link)
#     return comment

def create_jira_message(job_run_result_list, link):
    pattern = r'\((.*?)\)'
    comment = "This Issue was automatically created by Unravel to follow up on the insights generated for the runs of the jobs mentioned in the description of pr number {} was raised to merge {} from {} to {} <br>".format(
        pr_number, pr_commit_id, pr_base_branch, pr_target_branch)

    if job_run_result_list:
        for r in job_run_result_list:
            comment += "<br>Details of the dbx job {}<br><br>".format(r["job_id"])
            comment += "Job ID: {}<br>".format(r["unravel_url"])
            comment += "Cluster ID: {}<br>".format(re.search(pattern, r['app_summary']['Cluster']).group(1))
            comment += "Spark App: {}<br>".format(re.search(pattern, r['app_summary']['Spark App']).group(1))
            comment += "Estimated Cost: {}<br>".format(r['app_summary']['Estimated cost'])
            comment += "Tags: {}<br>".format(r['app_summary']['Tags'])
            comment += "AutoScaling Info: {}<br>".format(r['app_summary']['Autoscale'])
            comment += "<br>"
            comment += "The following insights were generated<br><br>"

            if r["unravel_insights"]:
                for insight in r["unravel_insights"]:
                    categories = insight["categories"]

                    if categories:
                        for k in categories.keys():
                            instances = categories[k]["instances"]

                            if instances:
                                for i in instances:
                                    if i["key"].upper() != "SPARKAPPTIMEREPORT":
                                        comment += "{}: {} <br>".format(i["key"].upper(), events_map[i['key']])

    comment += '<br><br>For more detailed information, click this link: {}'.format(link)
    comment = html_to_jira_wiki(comment)  # Escape special characters
    #comment = "<p>" + comment.replace("\n", "<br>") + "</p>"  # Add <p> tags and replace newlines with <br>

    return comment


# %%
def main():
    # description_json = json.loads(pr_json['description'])
    # job_run_list = get_job_runs_from_description(pr_id, description_json)
    raw_description = get_pr_description()
    description = " ".join(raw_description.splitlines())
    description = re.sub(cleanRe, "", description)
    job_run_list = get_job_runs_from_description_as_text(pr_number, description)

    # start and end TS
    today = datetime.today()
    endDT = datetime(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=today.hour,
        second=today.second,
    )
    startDT = endDT - timedelta(days=14)
    start_time = startDT.astimezone().isoformat()
    end_time = endDT.astimezone().isoformat()
    print("start: " + start_time)
    print("end: " + end_time)

    job_run_result_list = []
    for run in job_run_list:
        gsp = run["workspace_id"] + "_" + run["job_id"] + "_" + run["run_id"]
        job_runs_json = search_summary_by_globalsearchpattern(
            unravel_url, unravel_token, start_time, end_time, gsp
        )

        if job_runs_json:
            """
            gsp_file = gsp + '_summary.json'
            with open(gsp_file, "w") as outfile:
              json.dump(job_runs_json, outfile)
            """
            clusterUId = job_runs_json[0]["clusterUid"]
            appId = job_runs_json[0]["sparkAppId"]
            print("clusterUid: " + clusterUId)
            print("sparkAppId: " + appId)

            result_json = search_analysis(unravel_url, unravel_token, clusterUId, appId)
            if result_json:
                """
                gsp_file = gsp + '_analysis.json'
                with open(gsp_file, "w") as outfile:
                  json.dump(result_json, outfile)
                """
                insights_json = result_json["insightsV2"]
                recommendation_json = result_json["recommendation"]
                insights2_json = []
                for item in insights_json:
                    # if item['key'] != 'SparkAppTimeReport':
                    insights2_json.append(item)

                run[
                    "unravel_url"
                ] = unravel_url + "/#/app/application/db?execId={}".format(gsp)
                run["unravel_insights"] = insights2_json
                run["unravel_recommendation"] = recommendation_json
                run["app_summary"] = fetch_app_summary(unravel_url, unravel_token, clusterUId, appId)
                # add to the list
                job_run_result_list.append(run)
        else:
            print("job_run not found: " + gsp)

    if job_run_result_list:
        # unravel_comments = re.sub(cleanRe, '', json.dumps(job_run_result_list, indent=4))
        unravel_comments = create_comments_with_markdown(job_run_result_list)

        url =  f"https://api.github.com/repos/{repo_name}/issues/{pr_number}/comments"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        payload = {"body": '{}'.format(unravel_comments)}
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        channel = '#cicd-notifications'
        # Replace with your Markdown-formatted message
        message = 'Unravel has insights for the pr number {} which was raised to merge {} from {} to {}. Click this link for further details {}'.format(
            pr_number, pr_commit_id, pr_base_branch, pr_target_branch, pr_url)

        send_markdown_to_slack(channel, message)

        jira_message = create_jira_message(job_run_result_list, pr_url)

        raise_jira_ticket(jira_message)

    else:
        print("Nothing to do without Unravel integration")
        sys.exit(0)


# %%
if __name__ == "__main__":
    main()
