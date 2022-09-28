import urllib
import time
from typing import cast, Dict, Optional

import looker_sdk
from looker_sdk import models

sdk = looker_sdk.init31('./old_code/Looker/Looker API test/looker.ini')


def download_dashboard(
    dashboard_id,
    style: str = "tiled",
    width: int = 545,
    height: int = 842,
    filters: Optional[Dict[str, str]] = None,
):
    """Download specified dashboard as PDF"""
    task = sdk.create_dashboard_render_task(
        dashboard_id,
        "pdf",
        models.CreateDashboardRenderTask(
            dashboard_style=style,
            dashboard_filters=urllib.parse.urlencode(
                filters) if filters else None,
        ),
        width,
        height,
    )

    if not (task and task.id):
        raise Exception(
            f"Could not create a render task for dashboard_id: {dashboard_id}"
        )

    # poll the render task until it completes
    elapsed = 0.0
    delay = 0.5  # wait .5 seconds
    while True:
        poll = sdk.render_task(task.id)
        if poll.status == "failure":
            print(poll)
            raise Exception(
                f"Render failed for dashboard_id: {dashboard_id}"
            )
        elif poll.status == "success":
            break

        time.sleep(delay)
        elapsed += delay

    result = sdk.render_task_results(task.id)
    filename = f"{filters['Partner ID']}.pdf"
    with open(f'./old_code/Looker/Looker API test/{filename}', "wb") as f:
        f.write(result)
    print(f"OK PDF: {filename} in {elapsed} seconds")


dashboard_id = 19919
partners = [161029, 141982]

for partner in partners:
    filters = {"Partner ID": "{}".format(partner)}
    pdf_style = "tiled"
    pdf_width = 545
    pdf_height = 842
    download_dashboard(dashboard_id, pdf_style, pdf_width, pdf_height, filters)
