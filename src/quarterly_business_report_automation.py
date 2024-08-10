from datetime import datetime, timedelta
import json
import os

from dotenv import load_dotenv
from loguru import logger
import opsgenie_sdk
from opsgenie_sdk import ApiException as OpsgenieApiException
from opsgenie_sdk import BaseAlert as OpsgenieBaseAlert
import pysnow
import requests
import smartsheet


# ====================== Environment / Global Variables =======================
load_dotenv(override=True)

# Initialize customer constant global variables.
CUSTOMER_CONFIG_JSON = os.getenv('CUSTOMER_CONFIGS')
CUSTOMER_CONFIGS = json.loads(CUSTOMER_CONFIG_JSON)

# Initialize Opsgenie constant global variables.
OPSGENIE_API_KEY = os.getenv('OPSGENIE_API_KEY')
OPSGENIE_MAX_RESPONSE_LIMIT = 100

# Initialize PRTG constant global variables.
PRTG_MAX_RESPONSE_LIMIT = 50000

# Initialize ServiceNow constant global variables.
SERVICENOW_INSTANCE_NAME = os.getenv('SERVICENOW_INSTANCE_NAME')
SERVICENOW_INSTANCE_URL = f'https://{SERVICENOW_INSTANCE_NAME}.service-now.com'
SERVICENOW_USERNAME = os.getenv('SERVICENOW_USERNAME')
SERVICENOW_PASSWORD = os.getenv('SERVICENOW_PASSWORD')
SERVICENOW_CLIENT = pysnow.Client(
    instance=SERVICENOW_INSTANCE_NAME,
    user=SERVICENOW_USERNAME,
    password=SERVICENOW_PASSWORD
)
SERVICENOW_CLIENT.parameters.display_value = True

# Initialize Smartsheet constant global variables.
SMARTSHEET_API_KEY = os.getenv('SMARTSHEET_API_KEY')
SMARTSHEET_CLIENT = smartsheet.Smartsheet(access_token=SMARTSHEET_API_KEY)
SMARTSHEET_MAX_ROW_DELETION = 100


# ================================== Classes ==================================
class OpsgenieClient:
    """
    Represents a connection to an Opsgenie instance.
    """

    def __init__(self):
        """
        Initializes a connection to an Opsgenie instance and configures the
        SDK.
        """

        # Initialize configuration of the Opsgenie SDK.
        self.conf = opsgenie_sdk.configuration.Configuration()
        self.conf.api_key['Authorization'] = OPSGENIE_API_KEY
        self.api_client = opsgenie_sdk.api_client.ApiClient(configuration=self.conf)

        # Initialize needed API endpoints.
        self.alert_api = opsgenie_sdk.AlertApi(api_client=self.api_client)

    def paginate_opsgenie_alerts(self, query: str):
        """
        Generator function that will paginate over a list of Opsgenie alerts 
        based off the provided query. Returns a list of Opsgenie BaseAlert
        objects.

        Args:
            query (str): The query string to send to Opsgenie. More information
                for how to format an Opsgenie query can be found here:
                https://support.atlassian.com/opsgenie/docs/search-queries-for-alerts/

        Returns:
            (list[BaseAlert]): A list of Opsgenie BaseAlert objects.

        Yields:
            (list[BaseAlert]): A list of Opsgenie BaseAlert objects.
        """

        # Keep track of the offset from the results for pagination.
        current_offset = 0

        # Get the first page of the response.
        try:
            list_alerts_response = self.alert_api.list_alerts(
                limit=OPSGENIE_MAX_RESPONSE_LIMIT,
                order='desc',
                query=query
            )
        except OpsgenieApiException as og_api_exception:
            logger.error("An exception occurred when calling the Opsgenie " \
                         "AlertApi->list_alerts endpoint: %s\n" % og_api_exception)

        # Check if there is not a next page.
        if list_alerts_response.paging.next is None:
            # Return the first (and only) page of alert data.
            return list_alerts_response.data
        
        # Return the first page of data.
        yield list_alerts_response.data

        # While there are more pages, keep paginating the alerts response.
        while list_alerts_response.paging.next is not None:
            # Get the offset for the next page.
            current_offset += OPSGENIE_MAX_RESPONSE_LIMIT

            # Get the next page of the alerts response.
            try:
                list_alerts_response = self.alert_api.list_alerts(
                    limit=OPSGENIE_MAX_RESPONSE_LIMIT,
                    offset=current_offset,
                    order='desc',
                    query=query
                )
            except OpsgenieApiException as og_api_exception:
                logger.error("An exception occurred when calling the Opsgenie " \
                             "AlertApi->list_alerts endpoint: %s\n" % og_api_exception)
            
            # Return the next page of the alerts response.
            yield list_alerts_response.data


# ================================= Functions =================================
def clear_smartsheet(smartsheet_sheet: smartsheet.Smartsheet.models.sheet.Sheet) -> None:
    """
    Clears all rows in the provided Smartsheet.

    Params:
        smartsheet_sheet (smartsheet.Smartsheet.models.sheet): The Smartsheet 
            to clear all the rows for.
    """

    logger.info(f'Clearing Smartsheet "{smartsheet_sheet.name}"...')

    # Gather all row IDs in the Smartsheet.
    all_row_ids = []
    for row in smartsheet_sheet.rows:
        # Add this row's ID to the row ID list.
        all_row_ids.append(row.id)
    
    # Check if the Smartsheet is already empty.
    if len(all_row_ids) == 0:
        logger.info('Smartsheet already empty!')
        return

    # Clear the Smartsheet in chunks.
    chunk_size = 100
    for chunk_offset in range(0, len(all_row_ids), chunk_size):
        # Get the row ID chunk.
        row_id_chunk = all_row_ids[chunk_offset:chunk_offset + chunk_size]

        # Clear this chunk of rows in the Smartsheet.
        clear_row_chunk_response = SMARTSHEET_CLIENT.Sheets.delete_rows(
            smartsheet_sheet.id,
            row_id_chunk
        )

        # Check if the clearing failed and try again.
        if clear_row_chunk_response.message != 'SUCCESS':
            logger.error(f'An error occurred while trying to clear a chunk of '
                         f'rows from the "{smartsheet_sheet.name}" Smartsheet')
            logger.error(f'Result Code: {clear_row_chunk_response.result.code}')
            logger.info('Trying again...')
            continue

    logger.info(f'All rows in the "{smartsheet_sheet.name}" Smartsheet were '
                f'cleared successfully!')
    

def add_rows_to_smartsheet(smartsheet_sheet: smartsheet.Smartsheet.models.sheet.Sheet, 
                           rows: list[smartsheet.Smartsheet.models.row.Row]) -> None:
    """
    Adds the provided list of rows to the provided Smartsheet.

    Args:
        smartsheet_sheet (smartsheet.Smartsheet.models.sheet): The Smartsheet
            to add the rows to.
        rows (list[smartsheet.Smartsheet.models.row.Row]): The rows to add to
            the Smartsheet.
    """

    logger.info(f'Adding rows to the "{smartsheet_sheet.name}" Smartsheet...')

    # Add all the rows to the Smartsheet.
    add_all_rows_response = SMARTSHEET_CLIENT.Sheets.add_rows(
        smartsheet_sheet.id,
        rows
    )

    # Output if the rows were added successfully or not.
    if add_all_rows_response.message == 'SUCCESS':
        logger.info(f'All rows in the "{smartsheet_sheet.name}" Smartsheet '
                    f'were added successfully!')
    else:
        logger.error(f'An error occurred while adding rows to the '
                     f'"{smartsheet_sheet.name}" Smartsheet')
        logger.error(f'Result Code: {add_all_rows_response.result.code}')


def opsgenie_alert_to_row(alert_data: OpsgenieBaseAlert, smartsheet_sheet: smartsheet.Smartsheet.models.sheet.Sheet) -> smartsheet.Smartsheet.models.row.Row:
    """
    Given an Opsgenie BaseAlert object and a valid Smartsheet object, convert
    the base alert's data into a Smartsheet row object.

    Args:
        alert_data (OpsgenieBaseAlert): The alert data we want to convert.
        sheet (Smartsheet Sheet Object): The Smartsheet to create the Row
            for.

    Returns:
        row (Smartsheet Row Object): The Smartsheet row object containing the 
            alert's data.
    """

    # Initialize the row object we will be returning.
    alert_row = smartsheet.models.Row()
    alert_row.to_top = True

    # Initialize the cell with the alert's alias.
    alias_cell = smartsheet.models.Cell()
    alias_cell.column_id = smartsheet_sheet.columns[0].id
    alias_cell.value = alert_data.alias

    # Initialize the cell with the alert's message.
    message_cell = smartsheet.models.Cell()
    message_cell.column_id = smartsheet_sheet.columns[1].id
    message_cell.value = alert_data.message

    # Initialize the cell with the alert's ID.
    id_cell = smartsheet.models.Cell()
    id_cell.column_id = smartsheet_sheet.columns[2].id
    id_cell.value = alert_data.id

    # Initialize the cell with the alert's creation time.
    created_at_cell = smartsheet.models.Cell()
    created_at_cell.column_id = smartsheet_sheet.columns[3].id
    created_at_cell.value = alert_data.created_at.isoformat()

    # Initialize the cell with the alert's acknowledgement status.
    ack_cell = smartsheet.models.Cell()
    ack_cell.column_id = smartsheet_sheet.columns[4].id
    ack_cell.value = str(alert_data.acknowledged)

    # Initialize the cell with the alert's status.
    status_cell = smartsheet.models.Cell()
    status_cell.column_id = smartsheet_sheet.columns[5].id
    status_cell.value = alert_data.status

    # Initialize the cell with the alert's source.
    source_cell = smartsheet.models.Cell()
    source_cell.column_id = smartsheet_sheet.columns[6].id
    source_cell.value = alert_data.source

    # Initialize the cell with the alert's count.
    count_cell = smartsheet.models.Cell()
    count_cell.column_id = smartsheet_sheet.columns[7].id
    count_cell.value = str(alert_data.count)

    # Initialize the cell with the alert's priority.
    priority_cell = smartsheet.models.Cell()
    priority_cell.column_id = smartsheet_sheet.columns[8].id
    priority_cell.value = alert_data.priority

    # Update the row object with the all the cell objects.
    alert_row.cells.append(alias_cell)
    alert_row.cells.append(message_cell)
    alert_row.cells.append(id_cell)
    alert_row.cells.append(created_at_cell)
    alert_row.cells.append(ack_cell)
    alert_row.cells.append(status_cell)
    alert_row.cells.append(source_cell)
    alert_row.cells.append(count_cell)
    alert_row.cells.append(priority_cell)

    # Return the row.
    return alert_row


def get_quarterly_opsgenie_alerts(opsgenie_alert_tag: str) -> list[OpsgenieBaseAlert]:
    """
    Given a valid Opsgenie tag, return all alerts within the past 90 days with
    provided tag.

    Args:
        opsgenie_alert_tag (str): The tag associated with the desired alerts.

    Returns:
        list[OpsgenieBaseAlert]: A list of quarterly alerts with the associated
            alert tag.
    """

    logger.info('Gathering quarterly Opsgenie alert data...')

    # Establish a connection to our Opsgenie instance.
    opsgenie_client = OpsgenieClient()
    
    # Create a query for Opsgenie to get quarterly alerts.
    date_90_days_ago = datetime.today() - timedelta(days=90)
    quarterly_alerts_query = \
        f'createdAt >= {date_90_days_ago.strftime("%d-%m-%Y")} ' \
        f'tag: "{opsgenie_alert_tag}"'

    # Paginate over the quarterly Opsgenie alerts.
    quarterly_alerts = []
    for opsgenie_alerts_page in opsgenie_client.paginate_opsgenie_alerts(quarterly_alerts_query):
        # Add this alert to the list of quarterly alerts.
        for opsgenie_alert in opsgenie_alerts_page:
            quarterly_alerts.append(opsgenie_alert)
    
    logger.info('Opsgenie quarterly alert data gathered!')

    # Return all the quarterly alerts.
    return quarterly_alerts


def convert_opsgenie_alerts_to_smartsheet_rows(opsgenie_alerts: list[OpsgenieBaseAlert], smartsheet_sheet: smartsheet.Smartsheet.models.sheet.Sheet) -> list[smartsheet.Smartsheet.models.row.Row]:
    """
    Given a list of Opsgenie base alert objects and a desired Smartsheet sheet 
    object, convert the list of alerts to a list of Smartsheet row objects and
    return the list of rows.

    Args:
        opsgenie_alerts (list[OpsgenieBaseAlert]): The list of Opsgenie alerts
            to convert to Smartsheet rows.
        smartsheet_sheet (smartsheet.Smartsheet.models.sheet.Sheet): The 
            desired Smartsheet the alerts should go into.

    Returns:
        list[smartsheet.Smartsheet.models.row.Row]: The list of rows of the 
        converted alert objects.
    """

    # For each opsgenie alert, convert it into a Smartsheet row and add it to
    # the returning list of Smartsheet rows.
    all_alert_rows = []
    for opsgenie_alert in opsgenie_alerts:
        opsgenie_alert_row = opsgenie_alert_to_row(opsgenie_alert, smartsheet_sheet)
        all_alert_rows.append(opsgenie_alert_row)
    
    # Return all the alert rows.
    return all_alert_rows


def put_opsgenie_data_into_smartsheet(customer_config: dict) -> None:
    """
    Given a customer configuration, get quarterly Opsgenie alert data and push
    it into a Smartsheet.

    Args:
        customer_config (dict): The customer's configuration.
    """

    # Get the quarterly Opsgenie data for this customer.
    quarterly_opsgenie_alerts = get_quarterly_opsgenie_alerts(customer_config['opsgenie_tag'])

    # Get a reference to this customer's Opsgenie alerts Smartsheet.
    opsgenie_smartsheet = SMARTSHEET_CLIENT.Sheets.get_sheet(customer_config['smartsheet_opsgenie_alerts_sheet_id'])

    # Convert the alerts to Smartsheet rows.
    quarterly_opsgenie_alerts_rows = convert_opsgenie_alerts_to_smartsheet_rows(quarterly_opsgenie_alerts, opsgenie_smartsheet)

    # Clear the Smartsheet before pushing the fresh data.
    clear_smartsheet(opsgenie_smartsheet)

    # Add all the rows to Smartsheet.
    add_rows_to_smartsheet(opsgenie_smartsheet, quarterly_opsgenie_alerts_rows)


def servicenow_ticket_to_row(ticket_data: dict, smartsheet_sheet: smartsheet.Smartsheet.models.sheet.Sheet) -> smartsheet.Smartsheet.models.row.Row:
    """
    Given ServiceNow ticket data and a valid Smartsheet sheet object, convert 
    the ticket's data into a Smartsheet row object.

    Args:
        ticket_data (dict): The ticket data we want to convert.
        smartsheet_sheet (smartsheet.Smartsheet.models.sheet.Sheet): The
            Smartsheet to create the rows for.

    Returns:
        smartsheet.Smartsheet.models.row.Row: The Smartsheet row object
            containing the ticket's data.
    """

    # Initialize the row object we will be returning.
    ticket_row = smartsheet.models.Row()
    ticket_row.to_top = True

    # Initialize the cell with the ticket's number.
    number_cell = smartsheet.models.Cell()
    number_cell.column_id = smartsheet_sheet.columns[0].id
    number_cell.value = ticket_data['number']

    # Initialize the cell with the ticket's location.
    location_cell = smartsheet.models.Cell()
    location_cell.column_id = smartsheet_sheet.columns[1].id
    location_cell.value = ticket_data['location.name']

    # Initialize the cell with the ticket's CMDB CI name.
    ci_cell = smartsheet.models.Cell()
    ci_cell.column_id = smartsheet_sheet.columns[2].id
    ci_cell.value = ticket_data['cmdb_ci.name']

    # Initialize the cell with the ticket's short description.
    short_description_cell = smartsheet.models.Cell()
    short_description_cell.column_id = smartsheet_sheet.columns[3].id
    short_description_cell.value = ticket_data['short_description']

    # Initialize the cell with the ticket's state.
    state_cell = smartsheet.models.Cell()
    state_cell.column_id = smartsheet_sheet.columns[4].id
    state_cell.value = ticket_data['state']

    # Initialize the cell with the ticket's category.
    category_cell = smartsheet.models.Cell()
    category_cell.column_id = smartsheet_sheet.columns[5].id
    if ticket_data.get('category', None) is None:
        category_cell.value = ''
    else:
        category_cell.value = ticket_data['category']

    # Initialize the cell with the ticket's priority.
    priority_cell = smartsheet.models.Cell()
    priority_cell.column_id = smartsheet_sheet.columns[6].id
    priority_cell.value = ticket_data['priority']

    # Initialize the cell with the ticket's risk.
    risk_cell = smartsheet.models.Cell()
    risk_cell.column_id = smartsheet_sheet.columns[7].id
    if ticket_data.get('risk', None) is None:
        risk_cell.value = ''
    else:
        risk_cell.value = ticket_data['risk']

    # Initialize the cell with the ticket's assigned to.
    assigned_to_cell = smartsheet.models.Cell()
    assigned_to_cell.column_id = smartsheet_sheet.columns[8].id
    assigned_to_cell.value = ticket_data['assigned_to.name']

    # Initialize the cell with the ticket's opened at time.
    opened_at_cell = smartsheet.models.Cell()
    opened_at_cell.column_id = smartsheet_sheet.columns[9].id
    opened_at_cell.value = ticket_data['opened_at']

    # Initialize the cell with the ticket's updated by.
    updated_by_cell = smartsheet.models.Cell()
    updated_by_cell.column_id = smartsheet_sheet.columns[10].id
    updated_by_cell.value = ticket_data['sys_updated_by']

    # Initialize the cell with the ticket's close time.
    closed_at_cell = smartsheet.models.Cell()
    closed_at_cell.column_id = smartsheet_sheet.columns[11].id
    closed_at_cell.value = ticket_data['closed_at']

    # Update the row object with the all the cell objects.
    ticket_row.cells.append(number_cell)
    ticket_row.cells.append(location_cell)
    ticket_row.cells.append(ci_cell)
    ticket_row.cells.append(short_description_cell)
    ticket_row.cells.append(state_cell)
    ticket_row.cells.append(category_cell)
    ticket_row.cells.append(priority_cell)
    ticket_row.cells.append(risk_cell)
    ticket_row.cells.append(assigned_to_cell)
    ticket_row.cells.append(opened_at_cell)
    ticket_row.cells.append(updated_by_cell)
    ticket_row.cells.append(closed_at_cell)

    # Return the row.
    return ticket_row


def get_quarterly_servicenow_tickets(servicenow_company_name: str) -> list[dict]:
    """
    Given a valid ServiceNow company name, return all supported ticket types
    within the past 90 days.

    Args:
        servicenow_company_name (str): The company to gather quarterly ticket
            data for.

    Returns:
        list[dict]: A list of quarterly tickets for the company.
    """

    logger.info('Gathering quarterly ServiceNow ticket data...')

    # Get relevant ServiceNow tables.
    servicenow_incident_table = SERVICENOW_CLIENT.resource(api_path='/table/incident')
    servicenow_request_item_table = SERVICENOW_CLIENT.resource(api_path='/table/sc_req_item')
    servicenow_change_request_table = SERVICENOW_CLIENT.resource(api_path='/table/change_request')

    # Build the query to get the quarterly tickets from the tables.
    date_90_days_ago = datetime.today() - timedelta(days=90)
    tickets_last_90_days_query = (
        pysnow.QueryBuilder()
        .field('company.name').equals(servicenow_company_name)
        .AND()
        .field('sys_created_on').greater_than_or_equal(date_90_days_ago)
    )

    # Gather quarterly ticket data from the incident table.
    servicenow_quarterly_incidents_response = servicenow_incident_table.get(
        query=tickets_last_90_days_query,
        fields=['number', 'location.name', 'cmdb_ci.name', 'short_description',
                'state', 'category', 'priority', 'risk', 'assigned_to.name',
                'opened_at', 'sys_updated_by', 'closed_at']
    )
    servicenow_quarterly_incidents = servicenow_quarterly_incidents_response.all()

    # Gather quarterly ticket data from the request item table.
    servicenow_quarterly_request_items_response = servicenow_request_item_table.get(
        query=tickets_last_90_days_query,
        fields=['number', 'location.name', 'cmdb_ci.name', 'short_description',
                'state', 'category', 'priority', 'risk', 'assigned_to.name',
                'opened_at', 'sys_updated_by', 'closed_at']
    )
    servicenow_quarterly_request_items = servicenow_quarterly_request_items_response.all()

    # Gather quarterly ticket data from the change request table.
    servicenow_quarterly_change_requests_response = servicenow_change_request_table.get(
        query=tickets_last_90_days_query,
        fields=['number', 'location.name', 'cmdb_ci.name', 'short_description',
                'state', 'category', 'priority', 'risk', 'assigned_to.name',
                'opened_at', 'sys_updated_by', 'closed_at']
    )
    servicenow_quarterly_change_requests = servicenow_quarterly_change_requests_response.all()

    # Combine all quarterly ticket lists into a single list.
    all_quarterly_tickets = (
        servicenow_quarterly_incidents + 
        servicenow_quarterly_request_items + 
        servicenow_quarterly_change_requests
    )

    # Sort the quarterly tickets by date opened (latest tickets at the top).
    all_quarterly_tickets = sorted(all_quarterly_tickets, key=lambda ticket: datetime.strptime(ticket['opened_at'], '%Y-%m-%d %I:%M:%S %p'))
    all_quarterly_tickets.reverse()

    logger.info('ServiceNow quarterly ticket data gathered!')

    # Return the quarterly tickets for this customer.
    return all_quarterly_tickets


def convert_servicenow_tickets_to_smartsheet_rows(servicenow_tickets: list[dict], smartsheet_sheet: smartsheet.Smartsheet.models.sheet.Sheet) -> list[smartsheet.Smartsheet.models.row.Row]:
    """
    Given a list of ServiceNow tickets and a desired Smartsheet sheet object,
    convert the list of tickets to a list of Smartsheet row objects and return
    the list of rows.

    Args:
        servicenow_tickets (list[dict]): The list of ServiceNow tickets to
            convert to Smartsheet rows.
        smartsheet_sheet (smartsheet.Smartsheet.models.sheet.Sheet): The
            desired Smartsheet the tickets should go into.
    
    Returns:
        list[smartsheet.Smartsheet.models.row.Row]: The list of rows of the
            converted tickets.
    """

    # For each ticket, convert it into a Smartsheet row and add it to the
    # returning list of Smartsheet rows.
    all_ticket_rows = []
    for servicenow_ticket in servicenow_tickets:
        servicenow_ticket_row = servicenow_ticket_to_row(servicenow_ticket, smartsheet_sheet)
        all_ticket_rows.append(servicenow_ticket_row)

    # Return all the ticket rows.
    return all_ticket_rows


def put_servicenow_data_into_smartsheet(customer_config: dict) -> None:
    """
    Given a customer configuration, get quarterly ServiceNow ticket data and
    push it into a Smartsheet.

    Args:
        customer_config (dict): The customer's configuration.
    """

    # Get the quarterly ServiceNow tickets for this customer.
    quarterly_servicenow_tickets = get_quarterly_servicenow_tickets(customer_config['servicenow_company_name'])

    # Get a reference to this customer's ServiceNow ticket Smartsheet.
    servicenow_smartsheet = SMARTSHEET_CLIENT.Sheets.get_sheet(customer_config['smartsheet_servicenow_tickets_sheet_id'])

    # Convert the alerts to Smartsheet rows.
    quarterly_servicenow_tickets_rows = convert_servicenow_tickets_to_smartsheet_rows(quarterly_servicenow_tickets, servicenow_smartsheet)

    # Clear the Smartsheet before pushing the fresh data.
    clear_smartsheet(servicenow_smartsheet)

    # Add all the rows to Smartsheet.
    add_rows_to_smartsheet(servicenow_smartsheet, quarterly_servicenow_tickets_rows)


def prtg_sensor_to_row(prtg_sensor: dict, smartsheet_sheet: smartsheet.Smartsheet.models.sheet.Sheet) -> smartsheet.Smartsheet.models.row.Row:
    """
    Given a PRTG sensor object, convert the sensor's data into a Smartsheet row
    object.

    Args:
        prtg_sensor (dict): The sensor data we want to convert.
        smartsheet_sheet (smartsheet.Smartsheet.models.sheet.Sheet): The 
            Smartsheet we want to insert the row into.

    Returns:
        (smartsheet.Smartsheet.models.row.Row): The Smartsheet row object 
            containing the sensor's data.
    """

    # Initialize the row object we will be returning.
    sensor_row = smartsheet.models.Row()
    sensor_row.to_top = True

    # Initialize the cell with the sensor's status.
    status_cell = smartsheet.models.Cell()
    status_cell.column_id = smartsheet_sheet.columns[0].id
    status_cell.value = prtg_sensor['status']

    # Initialize the cell with the sensor's occurrance timestamp.
    occurred_cell = smartsheet.models.Cell()
    occurred_cell.column_id = smartsheet_sheet.columns[1].id
    occurred_cell.value = prtg_sensor['downtimesince']

    # Initialize the cell with the sensor's name.
    name_cell = smartsheet.models.Cell()
    name_cell.column_id = smartsheet_sheet.columns[2].id
    name_cell.value = prtg_sensor['name']

    # Initialize the cell with the sensor's probe / group / device.
    probe_group_device_cell = smartsheet.models.Cell()
    probe_group_device_cell.column_id = smartsheet_sheet.columns[3].id
    probe_group_device_cell.value = prtg_sensor['probe'] + ' > ' + \
        prtg_sensor['group'] + ' > ' + prtg_sensor['device']

    # Initialize the cell with the sensor's message.
    message_cell = smartsheet.models.Cell()
    message_cell.column_id = smartsheet_sheet.columns[4].id
    message_cell.value = prtg_sensor['message_raw']

    # Update the row object with the all the cell objects.
    sensor_row.cells.append(status_cell)
    sensor_row.cells.append(occurred_cell)
    sensor_row.cells.append(name_cell)
    sensor_row.cells.append(probe_group_device_cell)
    sensor_row.cells.append(message_cell)

    # Return the row.
    return sensor_row


def get_alerting_prtg_sensors(prtg_instance_urls: list[str], prtg_usernames: list[str], prtg_passhashs: list[str], prtg_probe_substrings: list[str]) -> list[dict]:
    """
    Return all non-online sensors from all provided PRTG instances with their
    respective credentials from the provided probe names.

    Args:
        prtg_instance_urls (list[str]): A list of all PRTG instances to gather
            non-online sensors for.
        prtg_usernames (list[str]): A list of PRTG usernames for their
            respective instances.
        prtg_passhashs (list[str]): A list of PRTG passhashs for their 
            respective usernames.
        prtg_probe_substrings (list[str]): A list of probe substrings to 
            specify which sensors to gather from amongst the PRTG instances.

    Returns:
        list[dict]: A list of PRTG sensor objects.
            Format:
            {
                'name': str,
                'name_raw': str,
                'parentid': int,
                'parentid_raw': int,
                'downtimesince': str,
                'downtimesince_raw': str,
                'status': str,
                'status_raw': int,
                'probe': str,
                'probe_raw': str,
                'group': str,
                'group_raw': str,
                'device': str,
                'device_raw': str,
                'message': str,
                'message_raw': str
            }
    """

    logger.info('Gathering PRTG sensor data...')

    # Get the sensors from each PRTG instance.
    all_prtg_sensors = []
    for index,prtg_instance_url in enumerate(prtg_instance_urls):
        prtg_sensors_resp = \
            requests.get(url=f"{prtg_instance_url}/api/table.xml",
                        params={
                            'content': 'sensors',
                            'columns': 'name,parentid,downtimesince,status,' \
                                       'probe,group,device,message',
                            'filter_probe': [f'@sub({probe_substring})' for probe_substring in prtg_probe_substrings],
                            'filter_status': '@neq(3)',
                            'output': 'json',
                            'count': str(PRTG_MAX_RESPONSE_LIMIT),
                            'username': prtg_usernames[index],
                            'passhash': prtg_passhashs[index]
                        }
            )
        
        # Add this PRTG instance's sensors to the customer's global sensor
        # list.
        prtg_sensors = prtg_sensors_resp.json()['sensors']
        all_prtg_sensors = all_prtg_sensors + prtg_sensors

    # Return all the PRTG sensor data.
    logger.info('PRTG sensor data gathered!')
    return all_prtg_sensors
    

def convert_prtg_sensors_to_smartsheet_rows(prtg_sensors: list[dict], smartsheet_sheet: smartsheet.Smartsheet.models.sheet.Sheet) -> list[smartsheet.Smartsheet.models.row.Row]:
    """
    Given a list of PRTG sensors and a desired Smartsheet sheet object, convert
    the list of sensors to a list of Smartsheet row objects and return the list
    of rows.

    Args:
        prtg_sensors (list[dict]): The list of PRTG sensors to convert to
            Smartsheet rows.
        smartsheet_sheet (smartsheet.Smartsheet.models.sheet.Sheet): The
            desired Smartsheet the sensors should go into.
    
    Returns:
        list[smartsheet.Smartsheet.models.row.Row]: The list of rows of the
            converted sensors.
    """
    
    # For each sensor, convert it into a Smartsheet row and add it to the
    # returning list of Smartsheet rows.
    all_sensor_rows = []
    for prtg_sensor in prtg_sensors:
        prtg_sensor_row = prtg_sensor_to_row(prtg_sensor, smartsheet_sheet)
        all_sensor_rows.append(prtg_sensor_row)

    # Return all the sensor rows.
    return all_sensor_rows


def put_prtg_sensor_data_into_smartsheet(customer_config: dict):
    """
    Given a customer configuration, get the current non-online PRTG sensor data
    and push it into a Smartsheet.

    Args:
        customer_config (dict): The customer's configuration.
    """

    # Get the current alerting PRTG sensors for this customer.
    current_alerting_prtg_sensors = get_alerting_prtg_sensors(
        customer_config['prtg_instance_urls'],
        customer_config['prtg_usernames'],
        customer_config['prtg_passhashs'],
        customer_config['prtg_probe_substrings']
    )

    # Get a reference to this customer's PRTG sensor Smartsheet.
    prtg_smartsheet = SMARTSHEET_CLIENT.Sheets.get_sheet(customer_config['smartsheet_prtg_alerts_sheet_id'])

    # Convert the sensors to Smartsheet rows.
    current_alerting_prtg_sensors_rows = convert_prtg_sensors_to_smartsheet_rows(current_alerting_prtg_sensors, prtg_smartsheet)

    # Clear the Smartsheet before pushing the fresh data.
    clear_smartsheet(prtg_smartsheet)

    # Add all the rows to Smartsheet.
    add_rows_to_smartsheet(prtg_smartsheet, current_alerting_prtg_sensors_rows)


def run():
    """
    Runs the Quarterly Business Report automation.
    """

    logger.info('Beginning QBR automation...')

    # Push all customer alert and ticket data into their respective Smartsheets.
    for customer_config in CUSTOMER_CONFIGS:
        logger.info(f'Beginning QBR automation for "{customer_config['opsgenie_tag']}"...')

        # Push this customer's Opsgenie alert data into a Smartsheet.
        put_opsgenie_data_into_smartsheet(customer_config)

        # Push this customer's ServiceNow tickets into a Smartsheet.
        put_servicenow_data_into_smartsheet(customer_config)

        # Push this customer's current PRTG sensor alerts into a Smartsheet.
        put_prtg_sensor_data_into_smartsheet(customer_config)

        logger.info(f'Completed QBR automation for "{customer_config['opsgenie_tag']}"!')
    
    logger.info('QBR automation completed successfully!')


if __name__ == "__main__":
    run()

    """
    Todo list:
        - For Opsgenie alerts, determine important tags (Storage, Networking,
            Backup, Server, <<AP>>, Other, etc.) and put them in a new column for the Opsgenie
            Smartsheets
        - ServiceNow date columns should be datetime objects
        - Hard-type PRTG sensors and ServiceNow tickets
        - Make ccxsapi PRTG account for unique customer's PRTG instances
        - Convert username / passhash for PRTG to API token
    """
