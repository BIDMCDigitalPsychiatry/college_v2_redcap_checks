import LAMP
import requests
LAMP.connect() # Please set env variables!

count = 0
for researcher in LAMP.Researcher.all()['data']:
    try:
        config = LAMP.Type.get_attachment(researcher['id'], 'org.digitalpsych.redcap.importer')['data']
        count += 1
    except LAMP.ApiException:
        continue # This Researcher is not configured for imports; ignore it.
    fields = {
        'token': config['API_TOKEN'],
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'exportSurveyFields': 'true'
    }
    records = requests.post(config['API_URL'], data=fields).json()
    LAMP.Type.set_attachment(researcher['id'], 'me',
                             attachment_key='org.digitalpsych.redcap.data',
                             body=records)
    print(count)
print("Finished importing data.")
