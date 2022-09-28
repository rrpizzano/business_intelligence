import tableauserverclient as TSC

tableau_server_url = "https://tableau.....net"
tableau_user = "...Tableau"
tableau_password = "6..."
tableau_site_id = "P..."

# connection to tableau server (if local use VPN)
try:
    tableau_auth = TSC.TableauAuth(
        tableau_user, tableau_password, tableau_site_id)
    server = TSC.Server(tableau_server_url, use_server_version=True)
    server.auth.sign_in(tableau_auth)
except Exception as err:
    raise Exception(
        f'Error while connecting to Tableau server, exception is: {str(err)}')


def get_view_id_by_name(workbook_name, workbook_view_name):

    workbook_id = None
    workbook_view_id = None

    with server.auth.sign_in(tableau_auth):

        req_options = TSC.RequestOptions().page_size(300)
        all_workbooks, pagination_item = server.workbooks.get(req_options)
        for workbook in [workbook for workbook in all_workbooks]:
            if workbook.name == workbook_name:
                workbook_id = workbook.id

        if workbook_id != None:
            for view in server.workbooks.get_by_id(workbook_id).views:
                if view.name == workbook_view_name:
                    workbook_view_id = view.id

    return workbook_view_id


# print(get_view_id_by_name('Matrix Overall', "KPI's Evolution"))
# Matrix Overall -> afd6c164-6a8b-4ceb-bc85-df91ec20e177
# Matrix Overall/KPI's Evolution -> 2aa2c492-227b-45db-9529-555fd89a9280
print(get_view_id_by_name("View name 1", "View name 2"))

server.auth.sign_out()
