import LAMP
import requests
import os
LAMP.connect() # Please set env variables!
import datetime

# ---------------------- Functions ---------------------- #
def check_participant_redcap(email):
    """ Check that the participant:
            - has completed the enrollment survey and passes
            - has completed ifc and passes
            - has uploaded ifc

        Args:
            email - the participant's student email
        Returns:
            -4 for no recent enrollment surveys / did not complete
            -3 for did not pass any enrollment surveys
            -2 for no informed consent surveys
            -1 for did not pass any informed consent surveys
            0 for did not upload ifc
            redcap index for all good!
    """
    COLLEGE_V2 = "4aq1kry81ktrb5v1smvs"
    # start timestamp is 11/1/2021
    START_TIMESTAMP = 1635739200000
    # Get the redcap data
    college_v2_redcap = LAMP.Type.get_attachment(COLLEGE_V2, 'org.digitalpsych.redcap.data')['data']
    # college_v2_redcap = college_v2_redcap.where(college_v2_redcap != '', None)
    df = [x for x in college_v2_redcap if x["student_email"].lower() == email.lower()]
    df = [x for x in df if x["enrollment_survey_timestamp"] != ""]
    converted_timestamps = []
    for i in range(len(df)):
        df[i]["converted_timestamp"] = int(datetime.datetime.strptime(df[i]["enrollment_survey_timestamp"], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    df = [x for x in df if x["converted_timestamp"] > START_TIMESTAMP]
    # no recent enrollment surveys
    if len(df) == 0:
        return -4
    # check to see if passed
    pss_pos_keys = ["pss1", "pss2", "pss3", "pss6", "pss9", "pss10"]
    pss_neg_keys = ["pss4", "pss5", "pss7", "pss8"]
    for i in range(len(df)):
        pss_sum = 0
        for k in pss_pos_keys:
            pss_sum += int(df[i][k])
        for k in pss_neg_keys:
            pss_sum -= int(df[i][k])
        df[i]["pss_sum"] = pss_sum + 16
    df = [x for x in df if (x["pss_sum"] >= 14 and int(x["year"]) != 4 and int(x["age"]) > 17)]
    if len(df) == 0:
        return -3
    ic_keys = ["ic1", "ic2", "ic3", "ic4", "ic5", "ic6", "ic7"]
    ind = []
    for i in range(len(df)):
        all_vals = True
        for k in ic_keys:
            if df[i][k] == "":
                all_vals = False
        if all_vals:
            ind.append(i)
    df = [df[i] for i in ind]
    if len(df) == 0:
        return -2
    passed = []
    for i in range(len(df)):
        df[i]["passed"] = _passed_ifc(df[i])
    df = [x for x in df if x["passed"]]
    if len(df) == 0:
        return -1
    for i in range(len(df)):
        if df[i]['ic_signed'] != "":
            return int(df[i]["record_id"])
    return 0

def _passed_ifc(df0):
    """ Helper function to check if an individual row of df passed the ifc

        Args:
            df0 - row of the dataframe
        Returns:
            0 for failed, 1 for pass
    """
    ic_keys = ["ic1", "ic2", "ic3", "ic4", "ic5", "ic6", "ic7",]
    corr_ans = [1, 0, 2, 0, 0, 1, 2]
    ic2_keys = ["ic1_v2", "ic2_v2", "ic3_v2", "ic4_v2", "ic5_v2", "ic6_v2", "ic7_v2",]
    corr_ans2 = [1, 0, 2, 0, 0, 1, 2]
    failed = False
    for kk, k in enumerate(ic_keys):
        if int(df0[k]) != corr_ans[kk]:
            failed = True
    if failed:
        for kk, k in enumerate(ic2_keys):
            if df0[k] == "":
                return 0
            if int(df0[k]) != corr_ans2[kk]:
                return 0
    return 1

def get_survey_links(redcap_id):
    """ Get the survey links for payment_auth and SUS.

        Args:
            redcap_id: the redcap id for a participant
        Returns:
            A dictionary in the form
                ret = {
                        "payment_authorization_1": {"done": 0, "link": ""},
                        "payment_authorization_2": {"done": 0, "link": ""},
                        "payment_authorization_3": {"done": 0, "link": ""},
                        "system_usability_scale": {"done": 0, "link": ""},
                      }
            where links are filled out and done is 1 if the survey is filled out
    """
    COLLEGE_V2 = "4aq1kry81ktrb5v1smvs"
    # Get the redcap data
    college_v2_redcap = LAMP.Type.get_attachment(COLLEGE_V2, 'org.digitalpsych.redcap.data')['data']
    # get id for this person
    df = [x for x in college_v2_redcap if x["record_id"] == str(redcap_id)][0]
    ret = {
        "payment_authorization_1": {"done": 0, "link": ""},
        "payment_authorization_2": {"done": 0, "link": ""},
        "payment_authorization_3": {"done": 0, "link": ""},
        "system_usability_scale": {"done": 0, "link": ""},
    }
    for k in ret.keys():
        if ret[k]["link"] == "":
            # Get the link
            data = {
                'token': config['API_TOKEN'],
                'content': 'surveyLink',
                'format': 'json',
                'instrument': k,
                'event': '',
                'record': redcap_id,
                'returnFormat': 'json'
            }
            r = requests.post(config['API_URL'], data=data)
            ret[k]["link"] = r.text
        if k == "system_usability_scale":
            if int(df[k + "_complete"]) == 2:
                ret[k]["done"] = 1
        else:
            num = k[-1]
            if int(df[k + "_complete"]) == 2 and df["payment_auth_" + num] != "":
                ret[k]["done"] = 1
    return ret
# ------------------------------------------------------------ #

# 1) Pull redcap data and attach to college
RESEARCHER = "4aq1kry81ktrb5v1smvs"
config = LAMP.Type.get_attachment(RESEARCHER, 'org.digitalpsych.redcap.importer')['data']
fields = {
            'token': config['API_TOKEN'],
            'content': 'record',
            'format': 'json',
            'type': 'flat',
            'exportSurveyFields': 'true'
         }
records = requests.post(config['API_URL'], data=fields).json()
LAMP.Type.set_attachment(RESEARCHER, 'me',
                            attachment_key='org.digitalpsych.redcap.data',
                            body=records)
print("Attached redcap data to researcher.")

# 2) Attach redcap ids for everyone in college

parts = []
for study in LAMP.Study.all_by_researcher(RESEARCHER)['data']:
    parts+=(p['id'] for p in LAMP.Participant.all_by_study(study['id'])['data'])

for p in parts:
    try:
        email = LAMP.Type.get_attachment(p, 'lamp.name')["data"]
        LAMP.Type.set_attachment(p, 'me',
                            attachment_key='org.digitalpsych.college_study_2.redcap_id',
                            body=check_participant_redcap(email))
    except LAMP.ApiException:
        # This participant does not have a name configured -- ignore
        continue
print("Attached redcap ids to participants.")

# 3) Attach survey links for everyone in college
for p in parts:
    try:
        redcap_id = LAMP.Type.get_attachment(p, 'org.digitalpsych.college_study_2.redcap_id')["data"]
        if redcap_id > 0:
            data = get_survey_links(redcap_id)
            LAMP.Type.set_attachment(p, 'me',
                            attachment_key='org.digitalpsych.college_study_2.payment_survey_links',
                            body=data)
    except LAMP.ApiException:
        # This participant does not have a name configured -- ignore
        continue
print("Attached survey data to participants.")

print("Done.")
