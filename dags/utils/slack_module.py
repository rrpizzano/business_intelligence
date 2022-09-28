def slack_ok(dag):
    """
    Returns an Airflow SlackWebhookOperator
    This SlackWebhookOperator sends a message to the Slack channel 'slack_channel_name_success' if the DAG was executed successfully

    Attributes
    ----------
        dag : Airflow dag
            Airflow dag that is generated with a config script
    """
    from airflow.utils.trigger_rule import TriggerRule
    from airflow.hooks.base_hook import BaseHook
    from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
    from datetime import datetime, timedelta
    return SlackWebhookOperator(
        dag=dag,
        task_id='slack_process_success',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        http_conn_id='slack_channel_name_success',
        webhook_token=BaseHook.get_connection(
            'slack_channel_name_success').password,
        message=f"""
                :large_green_circle: DAG Completed
                *DAG*: {dag.dag_id}
                *Finished at (UTC-3)*: {datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S')}
                """,
        username='airflow',
        retries=1
    )


def slack_fail(dag):
    """
    Returns an Airflow SlackWebhookOperator
    This SlackWebhookOperator sends a message to the Slack channel 'slack_channel_name_error' if the DAG was not executed successfully

    Attributes
    ----------
        dag : Airflow dag
            Airflow dag that is generated with a config script
    """
    from airflow.utils.trigger_rule import TriggerRule
    from airflow.hooks.base_hook import BaseHook
    from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
    from datetime import datetime, timedelta
    return SlackWebhookOperator(
        dag=dag,
        task_id='slack_process_error',
        trigger_rule=TriggerRule.ONE_FAILED,
        http_conn_id='slack_channel_name_error',
        webhook_token=BaseHook.get_connection(
            'slack_channel_name_error').password,
        message=f"""
                :red_circle: DAG Failed
                *DAG*: {dag.dag_id}
                *Execution time (UTC-3)*: {datetime.strftime(datetime.now() - timedelta(hours=3), '%Y-%m-%d %H:%M:%S')}
                """,
        username='airflow',
        retries=1
    )
