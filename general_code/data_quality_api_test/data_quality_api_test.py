
def get_table_etl_status(project, dataset, table):

    import requests
    import json

    headers = json.load(open('general_code/config/dq_api.json'))
    base_url = json.load(
        open('general_code/data_quality_api_test/config.json'))['BaseUrl']
    endpoint = base_url + '?projectID={}'.format(
        project) + '&datasetID={}'.format(dataset) + '&tableID={}'.format(table)
    response = requests.get(endpoint, headers=headers)
    data = json.loads(response.content)

    if 'status' in data and data['status'].upper() == 'FINALIZED':
        print(f"{project}.{dataset}.{table} >> {data['status']}")
    elif 'error' in data:
        print(f"{project}.{dataset}.{table} >> {data['error']}")


get_table_etl_status(
    project='peya-bi-tools-pro',
    dataset='il_core',
    table='dim_partner'
)

get_table_etl_status(
    project='peya-data-dev-tools-pro',
    dataset='external_process',
    table='braze_daily_data'
)

get_table_etl_status(
    project='peya-data-dev-tools-pro',
    dataset='external_process',
    table='partner_ops_braze_deleted_weekly_data'
)

get_table_etl_status(
    project='peya-delivery-and-support',
    dataset='user_maria_ugarte',
    table='Mkt_Comms_VP'
)

get_table_etl_status(
    project='peya-data-origins-pro',
    dataset='cl_billing_2',
    table='gsheet_contracts_with_proactive_policy_sap'
)
