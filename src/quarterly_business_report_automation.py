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
import smartsheet.sheets
from smartsheet.models.sheet import Sheet as SmartsheetSheet
from smartsheet.models.row import Row as SmartsheetRow
from smartsheet.models.cell import Cell as SmartsheetCell
from smartsheet import Smartsheet as SmartsheetClient


# ====================== Environment / Global Variables =======================
load_dotenv(override=True)

# Initialize customer constant global variables.
with open('/vault/secrets/qbr_auto', 'r') as file:
    CUSTOMER_CONFIGS_FILE_JSON = json.load(file)
CUSTOMER_CONFIGS_STRING = CUSTOMER_CONFIGS_FILE_JSON['data']['customer_configs']
CUSTOMER_CONFIGS = json.loads(CUSTOMER_CONFIGS_STRING)

# Initialize Opsgenie constant global variables.
OPSGENIE_API_KEY = os.getenv('OPSGENIE_API_KEY')
OPSGENIE_MAX_RESPONSE_LIMIT = 100

# Initialize PRTG constant global variables.
PRTG_01_USE_DEFAULTS_KEYWORD = 'prtg_01_default_instance'
PRTG_01_DEFAULT_INSTANCE_URL = os.getenv('PRTG_01_DEFAULT_INSTANCE_URL')
PRTG_01_DEFAULT_API_KEY = os.getenv('PRTG_01_DEFAULT_API_KEY')
PRTG_02_USE_DEFAULTS_KEYWORD = 'prtg_02_default_instance'
PRTG_02_DEFAULT_INSTANCE_URL = os.getenv('PRTG_02_DEFAULT_INSTANCE_URL')
PRTG_02_DEFAULT_API_KEY = os.getenv('PRTG_02_DEFAULT_API_KEY')
PRTG_MAX_RESPONSE_LIMIT = 50000

# Initialize ServiceNow constant global variables.
SERVICENOW_INSTANCE_NAME = os.getenv('SERVICENOW_INSTANCE_NAME')
SERVICENOW_USERNAME = os.getenv('SERVICENOW_USERNAME')
SERVICENOW_PASSWORD = os.getenv('SERVICENOW_PASSWORD')
SERVICENOW_CLIENT = pysnow.Client(
    instance=SERVICENOW_INSTANCE_NAME,
    user=SERVICENOW_USERNAME,
    password=SERVICENOW_PASSWORD
)
SERVICENOW_CLIENT.parameters.display_value = True
SERVICENOW_TICKET_FIELDS = [
    'number', 'location.name', 'cmdb_ci.name', 'short_description', 'state',
    'category', 'priority', 'risk', 'assigned_to.name', 'opened_at',
    'sys_updated_by', 'closed_at'
]
SERVICENOW_DATETIME_FORMAT = "%Y-%m-%d %I:%M:%S %p"

# Initialize Smartsheet constant global variables.
SMARTSHEET_API_KEY = os.getenv('SMARTSHEET_API_KEY')
SMARTSHEET_CLIENT = SmartsheetClient(access_token=SMARTSHEET_API_KEY)
SMARTSHEET_MAX_DASHBOARD_ROW_COUNT = 2500
SMARTSHEET_MAX_ROW_DELETION = 100

# Initialize other constant global variables.
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


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
        objects. This generator will stop providing results as soon as it 
        outputs SMARTSHEET_MAX_DASHBOARD_ROW_COUNT amount of alerts.

        Args:
            query (str): The query string to send to Opsgenie. More information
                for how to format an Opsgenie query can be found here:
                https://support.atlassian.com/opsgenie/docs/search-queries-for-alerts/

        Returns:
            list[BaseAlert]: A list of Opsgenie BaseAlert objects.

        Yields:
            list[BaseAlert]: A list of Opsgenie BaseAlert objects.
        """

        # Keep track of the offset from the results for pagination.
        current_offset = 0

        # Get the first page of the response.
        try:
            list_alerts_response = self.alert_api.list_alerts(
                limit=OPSGENIE_MAX_RESPONSE_LIMIT if OPSGENIE_MAX_RESPONSE_LIMIT <= SMARTSHEET_MAX_DASHBOARD_ROW_COUNT else SMARTSHEET_MAX_DASHBOARD_ROW_COUNT,
                order='desc',
                query=query
            )
        except OpsgenieApiException as og_api_exception:
            logger.error("An exception occurred when calling the Opsgenie " \
                         "AlertApi->list_alerts endpoint: %s\n" % og_api_exception)

        # Check if there is not a next page.
        if list_alerts_response.paging.next is None or len(list_alerts_response.data) == SMARTSHEET_MAX_DASHBOARD_ROW_COUNT:
            # Return the first (and only) page of alert data.
            return list_alerts_response.data
        
        # Return the first page of data.
        yield list_alerts_response.data

        # While there are more pages, keep paginating the alerts response.
        current_offset += OPSGENIE_MAX_RESPONSE_LIMIT
        while list_alerts_response.paging.next is not None and current_offset < SMARTSHEET_MAX_DASHBOARD_ROW_COUNT:
            # Get the next page of the alerts response.
            try:
                list_alerts_response = self.alert_api.list_alerts(
                    limit=OPSGENIE_MAX_RESPONSE_LIMIT
                        if (current_offset + OPSGENIE_MAX_RESPONSE_LIMIT) <= SMARTSHEET_MAX_DASHBOARD_ROW_COUNT
                        else (SMARTSHEET_MAX_DASHBOARD_ROW_COUNT - current_offset),
                    offset=current_offset,
                    order='desc',
                    query=query
                )
            except OpsgenieApiException as og_api_exception:
                logger.error("An exception occurred when calling the Opsgenie " \
                             "AlertApi->list_alerts endpoint: %s\n" % og_api_exception)
            
            # Return the next page of the alerts response.
            yield list_alerts_response.data
            
            # Get the offset for the next page.
            current_offset += OPSGENIE_MAX_RESPONSE_LIMIT


class ServiceNowTicket:
    """
    Represents a ticket in ServiceNow.
    """
    
    def __init__(self, number: str, location: str, cmdb_ci: str, short_description: str,
                 state: str, category: str, priority: str, risk: str, assigned_to: str,
                 opened_at: str, updated_by: str, closed_at: str):
        """
        Initializes a ticket from ServiceNow.

        Args:
            number (str): The ticket number.
            location (str): The location that the ticket is associated with.
            cmdb_ci (str): The name of the device the ticket is for.
            short_description (str): A short description why the ticket was
                created.
            state (str): The current state of the ticket.
            category (str): The category of device the ticket was made for.
            priority (str): The priority at which the ticket should be
                completed.
            risk (str): The risk associated with completing the ticket (for 
                "change request" tickets only, blank otherwise).
            assigned_to (str): The ServiceNow username the ticket was assigned
                to.
            opened_at (str): The date and time the ticket was opened at.
            updated_by (str): The ServiceNow username who last updated the
                ticket.
            closed_at (str): The date and time the ticket was closed at (if
                applicable, blank otherwise).
        """

        self.number = number
        self.location = location
        self.cmdb_ci = cmdb_ci
        self.short_description = short_description
        self.state = state
        self.category = '' if category is None else category
        self.priority = priority
        self.risk = '' if risk is None else risk
        self.assigned_to = assigned_to
        self.opened_at = datetime.strptime(opened_at, SERVICENOW_DATETIME_FORMAT)
        self.updated_by = updated_by
        self.closed_at = None if closed_at == '' else datetime.strptime(closed_at, SERVICENOW_DATETIME_FORMAT)
        
        # Determine resolve time by hand.
        if self.closed_at is None:
            self.resolve_time = ''
        else:
            resolve_time = self.closed_at - self.opened_at
            
            self.resolve_time = resolve_time.total_seconds() / 60 / 60 / 24


class PRTGSensor:
    """
    Represents a sensor from PRTG.
    """

    def __init__(self, name: str, parent_id: int, downtime_since: str, status: str,
                 probe: str, group: str, device: str, message: str):
        """
        Initializes a sensor from PRTG.

        Args:
            name (str): The name of the sensor.
            parent_id (int): The ID of the device the sensor is associated with.
            downtime_since (str): The amount of time this sensor has been down.
            status (str): The online status of the sensor.
            probe (str): The probe this sensor is connected to.
            group (str): The group this sensor is in.
            device (str): The device this sensor is for.
            message (str): The message from the sensor.
        """

        self.name = name
        self.parent_id = parent_id
        self.downtime_since = downtime_since
        self.status = status
        self.probe = probe
        self.group = group
        self.device = device
        self.message = message


# ================================= Functions =================================
def clear_smartsheet(smartsheet_sheet: SmartsheetSheet) -> None:
    """
    Clears all rows in the provided Smartsheet.

    Args:
        smartsheet_sheet (SmartsheetSheet): The Smartsheet 
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
    for chunk_offset in range(0, len(all_row_ids), SMARTSHEET_MAX_ROW_DELETION):
        # Get the row ID chunk.
        row_id_chunk = all_row_ids[chunk_offset:chunk_offset + SMARTSHEET_MAX_ROW_DELETION]

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
    

def delete_smartsheet_rows(smartsheet_sheet: SmartsheetSheet, smartsheet_rows: list[SmartsheetRow]) -> None:
    """
    Deletes the provided rows from the provided Smartsheet. The list of rows 
    must be rows that exist inside the Smartsheet.

    Args:
        smartsheet_sheet (SmartsheetSheet): The Smartsheet to delete the rows
            in.
        smartsheet_rows (list[SmartsheetRow]): The rows to delete from the
            Smartsheet.
    """

    logger.info(f'Deleting {len(smartsheet_rows)} rows from Smartsheet "{smartsheet_sheet.name}"...')

    # Extract the row IDs from the provided list of rows in the Smartsheet.
    all_row_ids = []
    for row in smartsheet_rows:
        # Add this row's ID to the row ID list.
        all_row_ids.append(row.id)
    
    # Check if the Smartsheet is already empty.
    if len(all_row_ids) == 0:
        logger.info('Smartsheet already empty!')
        return

    # Delete the rows in chunks.
    for chunk_offset in range(0, len(all_row_ids), SMARTSHEET_MAX_ROW_DELETION):
        # Get the row ID chunk.
        row_id_chunk = all_row_ids[chunk_offset:chunk_offset + SMARTSHEET_MAX_ROW_DELETION]

        # Delete this chunk of rows in the Smartsheet.
        delete_row_chunk_response = SMARTSHEET_CLIENT.Sheets.delete_rows(
            smartsheet_sheet.id,
            row_id_chunk
        )

        # Check if the deletion failed.
        if delete_row_chunk_response.message != 'SUCCESS':
            logger.error(f'An error occurred while trying to delete a chunk of '
                         f'rows from the "{smartsheet_sheet.name}" Smartsheet')
            logger.error(f'Result Code: {delete_row_chunk_response.result.code}')
            continue

    logger.info(f'{len(smartsheet_rows)} rows in the "{smartsheet_sheet.name}" Smartsheet were '
                f'deleted successfully!')


def add_rows_to_smartsheet(smartsheet_sheet: SmartsheetSheet, rows: list[SmartsheetRow]) -> None:
    """
    Adds the provided list of rows to the provided Smartsheet.

    Args:
        smartsheet_sheet (SmartsheetSheet): The Smartsheet
            to add the rows to.
        rows (list[SmartsheetRow]): The rows to add to
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


def determine_primary_opsgenie_tag(opsgenie_tags: list[str]) -> str:
    """
    Given a list of strings representing all the tags in an Opsgenie alert,
    return the primary tag. Priority is as follows:
    [server > network > backup > storage > replication > virtualization] > 
    [vcenter > ucs > host > data protection advisor > aps > contact center] > 
    [snow > probe device] > 
    [catchall > misc]

    Args:
        opsgenie_tags (list[str]): The list of tags from an Opsgenie alert.

    Returns:
        str: The primary tag from the list of tags given based on priority.
    """

    # Make all tags lowercase for easy comparison.
    opsgenie_tags_lowercase = [tag.lower() for tag in opsgenie_tags]

    # Edge case if the tag is unsupported.
    primary_tag = ""

    # Bottom priority tags.
    if "catchall" in opsgenie_tags_lowercase:
        primary_tag = "Catch-all"
    
    # Add special logic for getting tag substrings. This implementation
    # is extremely inefficient, but due to the state of our Opsgenie
    # tagging, this is the only way this can currently be done.
    for tag in opsgenie_tags_lowercase:
        if "hotline" in tag:
            primary_tag = "Hotline"
            break
        elif "vcenters" in tag:
            primary_tag = "vCenter"
            break
        elif "aps" in tag:
            primary_tag = "Access Point"
            break
        elif "-ap" in tag:
            primary_tag = "Access Point"
            break
        elif "hosts" in tag:
            primary_tag = "Host"
            break
        elif "data protection advisor" in tag:
            primary_tag = "Data Protection Advisor"
            break
        elif "ucs" in tag:
            primary_tag = "UCS"
            break
        elif "probe device" in tag:
            primary_tag = "Probe Device"
            break
        elif "snow" in tag:
            primary_tag = "ServiceNow"
            break
        elif "contactcenter" in tag:
            primary_tag = "Contact Center"
            break
        elif "virtualization" in tag:
            primary_tag = "Virtualization"
            break
        elif "repl" in tag:
            primary_tag = "Replication"
            break
        elif "storage" in tag:
            primary_tag = "Storage"
            break
        elif "-fabric" in tag:
            primary_tag = "Storage"
            break
        elif "bkup" in tag:
            primary_tag = "Backup"
            break
        elif "network" in tag:
            primary_tag = "Network"
            break
        elif "-sw" in tag:
            primary_tag = "Network"
            break
        elif "fw" in tag:
            primary_tag = "Network"
            break
        elif "server" in tag:
            primary_tag = "Server"
            break
        elif "hardware" in tag:
            primary_tag = "Hardware"
            break
    
    # If all else fails, the primary tag is miscellaneous.
    if primary_tag == "":
        primary_tag = "Misc."
    
    # Return the primary tag.
    return primary_tag


def opsgenie_alert_to_row(alert_data: OpsgenieBaseAlert, smartsheet_sheet: SmartsheetSheet) -> SmartsheetRow:
    """
    Given an Opsgenie BaseAlert object and a valid Smartsheet object, convert
    the base alert's data into a Smartsheet row object.

    Args:
        alert_data (OpsgenieBaseAlert): The alert data we want to convert.
        sheet (SmartsheetSheet): The Smartsheet to create the Row
            for.

    Returns:
        SmartsheetRow: The Smartsheet row object containing the 
            alert's data.
    """

    # Initialize the row object we will be returning.
    alert_row = SmartsheetRow()
    alert_row.to_top = True

    # Initialize the cell with the alert's alias.
    alias_cell = SmartsheetCell()
    alias_cell.column_id = smartsheet_sheet.columns[0].id
    alias_cell.value = alert_data.alias

    # Initialize the cell with the alert's type.
    type_cell = SmartsheetCell()
    type_cell.column_id = smartsheet_sheet.columns[1].id
    type_cell.value = determine_primary_opsgenie_tag(alert_data.tags)

    # Initialize the cell with the alert's message.
    message_cell = SmartsheetCell()
    message_cell.column_id = smartsheet_sheet.columns[2].id
    message_cell.value = alert_data.message

    # Initialize the cell with the alert's ID.
    id_cell = SmartsheetCell()
    id_cell.column_id = smartsheet_sheet.columns[3].id
    id_cell.value = alert_data.id

    # Initialize the cell with the alert's creation date (time will be
    # truncated away).
    created_at_date_only_cell = SmartsheetCell()
    created_at_date_only_cell.column_id = smartsheet_sheet.columns[4].id
    created_at_date_only_cell.value = alert_data.created_at.isoformat()

    # Initialize the cell with the alert's creation date and time.
    created_at_cell = SmartsheetCell()
    created_at_cell.column_id = smartsheet_sheet.columns[5].id
    created_at_cell.value = str(alert_data.created_at.strftime(TIMESTAMP_FORMAT))

    # Initialize the cell with the alert's acknowledgement status.
    ack_cell = SmartsheetCell()
    ack_cell.column_id = smartsheet_sheet.columns[6].id
    ack_cell.value = str(alert_data.acknowledged)

    # Initialize the cell with the alert's status.
    status_cell = SmartsheetCell()
    status_cell.column_id = smartsheet_sheet.columns[7].id
    status_cell.value = alert_data.status

    # Initialize the cell with the alert's source.
    source_cell = SmartsheetCell()
    source_cell.column_id = smartsheet_sheet.columns[8].id
    source_cell.value = alert_data.source

    # Initialize the cell with the alert's count.
    count_cell = SmartsheetCell()
    count_cell.column_id = smartsheet_sheet.columns[9].id
    count_cell.value = str(alert_data.count)

    # Initialize the cell with the alert's priority.
    priority_cell = SmartsheetCell()
    priority_cell.column_id = smartsheet_sheet.columns[10].id
    priority_cell.value = alert_data.priority

    # Update the row object with the all the cell objects.
    alert_row.cells.append(alias_cell)
    alert_row.cells.append(type_cell)
    alert_row.cells.append(message_cell)
    alert_row.cells.append(id_cell)
    alert_row.cells.append(created_at_date_only_cell)
    alert_row.cells.append(created_at_cell)
    alert_row.cells.append(ack_cell)
    alert_row.cells.append(status_cell)
    alert_row.cells.append(source_cell)
    alert_row.cells.append(count_cell)
    alert_row.cells.append(priority_cell)

    # Return the row.
    return alert_row


def get_quarterly_opsgenie_alerts(opsgenie_alert_tags: list[str]) -> list[OpsgenieBaseAlert]:
    """
    Given a valid list of Opsgenie tags, return all alerts within the past 90
    days with the provided tags.

    Args:
        opsgenie_alert_tags (list[str]): The tags associated with the desired
            alerts.

    Returns:
        list[OpsgenieBaseAlert]: A list of quarterly alerts with the associated
            alert tags.
    """

    logger.info('Gathering quarterly Opsgenie alert data...')

    # Establish a connection to our Opsgenie instance.
    opsgenie_client = OpsgenieClient()
    
    # Create a query for Opsgenie to get quarterly alerts.
    date_90_days_ago = datetime.today() - timedelta(days=90)
    quarterly_alerts_query = \
        f'createdAt >= {date_90_days_ago.strftime("%d-%m-%Y")} ' \
        f'tag: ("{"\" OR \"".join(opsgenie_alert_tags)}")'

    # Paginate over the quarterly Opsgenie alerts.
    quarterly_alerts = list[OpsgenieBaseAlert]()
    for opsgenie_alerts_page in opsgenie_client.paginate_opsgenie_alerts(quarterly_alerts_query):
        # Add this alert to the list of quarterly alerts.
        for opsgenie_alert in opsgenie_alerts_page:
            quarterly_alerts.append(opsgenie_alert)
    
    logger.info('Opsgenie quarterly alert data gathered!')

    # Return all the quarterly alerts.
    return quarterly_alerts


def convert_opsgenie_alerts_to_smartsheet_rows(opsgenie_alerts: list[OpsgenieBaseAlert], smartsheet_sheet: SmartsheetSheet) -> list[SmartsheetRow]:
    """
    Given a list of Opsgenie base alert objects and a desired Smartsheet sheet 
    object, convert the list of alerts to a list of Smartsheet row objects and
    return the list of rows.

    Args:
        opsgenie_alerts (list[OpsgenieBaseAlert]): The list of Opsgenie alerts
            to convert to Smartsheet rows.
        smartsheet_sheet (SmartsheetSheet): The 
            desired Smartsheet the alerts should go into.

    Returns:
        list[SmartsheetRow]: The list of rows of the 
        converted alert objects.
    """

    # For each opsgenie alert, convert it into a Smartsheet row and add it to
    # the returning list of Smartsheet rows.
    all_alert_rows = list[SmartsheetRow]()
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
    quarterly_opsgenie_alerts = get_quarterly_opsgenie_alerts(customer_config['opsgenie_tags'])

    # Get a reference to this customer's Opsgenie alerts Smartsheet.
    opsgenie_smartsheet = SMARTSHEET_CLIENT.Sheets.get_sheet(customer_config['smartsheet_sheet_ids']['opsgenie_alerts'])

    # Convert the alerts to Smartsheet rows.
    quarterly_opsgenie_alerts_rows = convert_opsgenie_alerts_to_smartsheet_rows(quarterly_opsgenie_alerts, opsgenie_smartsheet)

    # Clear the Smartsheet before pushing the fresh data.
    clear_smartsheet(opsgenie_smartsheet)

    # Add all the rows to Smartsheet.
    add_rows_to_smartsheet(opsgenie_smartsheet, quarterly_opsgenie_alerts_rows)


def servicenow_ticket_to_row(ticket_data: ServiceNowTicket, smartsheet_sheet: SmartsheetSheet) -> SmartsheetRow:
    """
    Given a ServiceNow ticket object and a valid Smartsheet sheet object, convert 
    the ticket's data into a Smartsheet row object.

    Args:
        ticket_data (ServiceNowTicket): The ticket data we want to convert.
        smartsheet_sheet (SmartsheetSheet): The
            Smartsheet to create the rows for.

    Returns:
        SmartsheetRow: The Smartsheet row object
            containing the ticket's data.
    """

    # Initialize the row object we will be returning.
    ticket_row = smartsheet.models.Row()
    ticket_row.to_top = True

    # Initialize the cell with the ticket's number.
    number_cell = SmartsheetCell()
    number_cell.column_id = smartsheet_sheet.columns[0].id
    number_cell.value = ticket_data.number

    # Initialize the cell with the ticket's location.
    location_cell = SmartsheetCell()
    location_cell.column_id = smartsheet_sheet.columns[1].id
    location_cell.value = ticket_data.location

    # Initialize the cell with the ticket's CMDB CI name.
    ci_cell = SmartsheetCell()
    ci_cell.column_id = smartsheet_sheet.columns[2].id
    ci_cell.value = ticket_data.cmdb_ci

    # Initialize the cell with the ticket's short description.
    short_description_cell = SmartsheetCell()
    short_description_cell.column_id = smartsheet_sheet.columns[3].id
    short_description_cell.value = ticket_data.short_description

    # Initialize the cell with the ticket's state.
    state_cell = SmartsheetCell()
    state_cell.column_id = smartsheet_sheet.columns[4].id
    state_cell.value = ticket_data.state

    # Initialize the cell with the ticket's category.
    category_cell = SmartsheetCell()
    category_cell.column_id = smartsheet_sheet.columns[5].id
    category_cell.value = ticket_data.category

    # Initialize the cell with the ticket's priority.
    priority_cell = SmartsheetCell()
    priority_cell.column_id = smartsheet_sheet.columns[6].id
    priority_cell.value = ticket_data.priority

    # Initialize the cell with the ticket's risk.
    risk_cell = SmartsheetCell()
    risk_cell.column_id = smartsheet_sheet.columns[7].id
    risk_cell.value = ticket_data.risk

    # Initialize the cell with the ticket's assigned to.
    assigned_to_cell = SmartsheetCell()
    assigned_to_cell.column_id = smartsheet_sheet.columns[8].id
    assigned_to_cell.value = ticket_data.assigned_to

    # Initialize the cell with the ticket's opened at date (time will be
    # truncated away).
    opened_at_date_cell = SmartsheetCell()
    opened_at_date_cell.column_id = smartsheet_sheet.columns[9].id
    opened_at_date_cell.value = ticket_data.opened_at.isoformat()

    # Initialize the cell with the ticket's opened at date and time.
    opened_at_datetime_cell = SmartsheetCell()
    opened_at_datetime_cell.column_id = smartsheet_sheet.columns[10].id
    opened_at_datetime_cell.value = str(ticket_data.opened_at.strftime(TIMESTAMP_FORMAT))

    # Initialize the cell with the ticket's updated by.
    updated_by_cell = SmartsheetCell()
    updated_by_cell.column_id = smartsheet_sheet.columns[11].id
    updated_by_cell.value = ticket_data.updated_by

    # Initialize the cell with the ticket's closed at date (time will be
    # truncated away).
    closed_at_date_cell = SmartsheetCell()
    closed_at_date_cell.column_id = smartsheet_sheet.columns[12].id
    closed_at_date_cell.value = '' if ticket_data.closed_at == None else ticket_data.closed_at.isoformat()

    # Initialize the cell with the ticket's closed at date and time.
    closed_at_datetime_cell = SmartsheetCell()
    closed_at_datetime_cell.column_id = smartsheet_sheet.columns[13].id
    closed_at_datetime_cell.value = '' if ticket_data.closed_at == None else str(ticket_data.closed_at.strftime(TIMESTAMP_FORMAT))

    # Initialize the cell with the ticket's resolution time in days.
    resolution_time_in_days_cell = SmartsheetCell()
    resolution_time_in_days_cell.column_id = smartsheet_sheet.columns[14].id
    resolution_time_in_days_cell.value = str(abs(round(ticket_data.resolve_time, 2))) if ticket_data.resolve_time != '' else ''

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
    ticket_row.cells.append(opened_at_date_cell)
    ticket_row.cells.append(opened_at_datetime_cell)
    ticket_row.cells.append(updated_by_cell)
    ticket_row.cells.append(closed_at_date_cell)
    ticket_row.cells.append(closed_at_datetime_cell)
    ticket_row.cells.append(resolution_time_in_days_cell)

    # Return the row.
    return ticket_row


def get_quarterly_servicenow_tickets(servicenow_company_names: list[str]) -> list[ServiceNowTicket]:
    """
    Given a valid ServiceNow company name, return all supported ticket types
    within the past 90 days.

    Args:
        servicenow_company_names (list[str]): The company names to gather
            quarterly ticket data for.

    Returns:
        list[ServiceNowTicket]: A list of quarterly tickets for the company.
    """

    logger.info('Gathering quarterly ServiceNow ticket data...')

    # Get relevant ServiceNow tables.
    servicenow_incident_table = SERVICENOW_CLIENT.resource(api_path='/table/incident')
    servicenow_request_item_table = SERVICENOW_CLIENT.resource(api_path='/table/sc_req_item')
    servicenow_change_request_table = SERVICENOW_CLIENT.resource(api_path='/table/change_request')

    # Build the query to get the quarterly tickets from the tables.
    date_90_days_ago = datetime.today() - timedelta(days=90)
    tickets_last_90_days_query = pysnow.QueryBuilder().field('sys_created_on').greater_than_or_equal(date_90_days_ago).AND()
    query_ends_with_and = True
    for company_name in servicenow_company_names:
        # Check if this is the first loop so we exclude the "OR".
        if query_ends_with_and:
            tickets_last_90_days_query = tickets_last_90_days_query.field('company.name').equals(company_name)
            query_ends_with_and = False
        else:
            tickets_last_90_days_query = tickets_last_90_days_query.OR().field('company.name').equals(company_name)

    # Gather quarterly ticket data from the incident table.
    servicenow_quarterly_incidents_response = servicenow_incident_table.get(
        query=tickets_last_90_days_query,
        fields=SERVICENOW_TICKET_FIELDS
    )
    servicenow_quarterly_incidents = servicenow_quarterly_incidents_response.all()

    # Gather quarterly ticket data from the request item table.
    servicenow_quarterly_request_items_response = servicenow_request_item_table.get(
        query=tickets_last_90_days_query,
        fields=SERVICENOW_TICKET_FIELDS
    )
    servicenow_quarterly_request_items = servicenow_quarterly_request_items_response.all()

    # Gather quarterly ticket data from the change request table.
    servicenow_quarterly_change_requests_response = servicenow_change_request_table.get(
        query=tickets_last_90_days_query,
        fields=SERVICENOW_TICKET_FIELDS
    )
    servicenow_quarterly_change_requests = servicenow_quarterly_change_requests_response.all()

    # Combine all quarterly ticket lists into a single list.
    all_raw_quarterly_tickets = (
        servicenow_quarterly_incidents + 
        servicenow_quarterly_request_items + 
        servicenow_quarterly_change_requests
    )

    # Convert all the raw ServiceNow ticket dictionaries to hard-typed ServiceNow ticket objects.
    all_servicenow_quarterly_tickets = list[ServiceNowTicket]()
    for servicenow_raw_ticket in all_raw_quarterly_tickets:
        servicenow_ticket = ServiceNowTicket(
            servicenow_raw_ticket['number'],
            servicenow_raw_ticket['location.name'],
            servicenow_raw_ticket['cmdb_ci.name'],
            servicenow_raw_ticket['short_description'],
            servicenow_raw_ticket['state'],
            servicenow_raw_ticket.get('category', None),
            servicenow_raw_ticket['priority'],
            servicenow_raw_ticket.get('risk', None),
            servicenow_raw_ticket['assigned_to.name'],
            servicenow_raw_ticket['opened_at'],
            servicenow_raw_ticket['sys_updated_by'],
            servicenow_raw_ticket['closed_at']
        )
        all_servicenow_quarterly_tickets.append(servicenow_ticket)

    # Sort the quarterly tickets by date opened (latest tickets at the top).
    all_servicenow_quarterly_tickets = sorted(all_servicenow_quarterly_tickets, key=lambda ticket: ticket.opened_at)
    all_servicenow_quarterly_tickets.reverse()

    logger.info('ServiceNow quarterly ticket data gathered!')

    # Return the quarterly tickets for this customer.
    if len(all_servicenow_quarterly_tickets) > SMARTSHEET_MAX_DASHBOARD_ROW_COUNT:
        return all_servicenow_quarterly_tickets[:SMARTSHEET_MAX_DASHBOARD_ROW_COUNT]
    
    return all_servicenow_quarterly_tickets


def convert_servicenow_tickets_to_smartsheet_rows(servicenow_tickets: list[ServiceNowTicket], smartsheet_sheet: SmartsheetSheet) -> list[SmartsheetRow]:
    """
    Given a list of ServiceNow tickets and a desired Smartsheet sheet object,
    convert the list of tickets to a list of Smartsheet row objects and return
    the list of rows.

    Args:
        servicenow_tickets (list[ServiceNowTicket]): The list of ServiceNow tickets to
            convert to Smartsheet rows.
        smartsheet_sheet (SmartsheetSheet): The
            desired Smartsheet the tickets should go into.
    
    Returns:
        list[SmartsheetRow]: The list of rows of the
            converted tickets.
    """

    # For each ticket, convert it into a Smartsheet row and add it to the
    # returning list of Smartsheet rows.
    all_ticket_rows = list[SmartsheetRow]()
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
    quarterly_servicenow_tickets = get_quarterly_servicenow_tickets(customer_config['servicenow_company_names'])

    # Get a reference to this customer's ServiceNow ticket Smartsheet.
    servicenow_smartsheet = SMARTSHEET_CLIENT.Sheets.get_sheet(customer_config['smartsheet_sheet_ids']['servicenow_tickets'])

    # Convert the alerts to Smartsheet rows.
    quarterly_servicenow_tickets_rows = convert_servicenow_tickets_to_smartsheet_rows(quarterly_servicenow_tickets, servicenow_smartsheet)

    # Clear the Smartsheet before pushing the fresh data.
    clear_smartsheet(servicenow_smartsheet)

    # Add all the rows to Smartsheet.
    add_rows_to_smartsheet(servicenow_smartsheet, quarterly_servicenow_tickets_rows)


def prtg_sensor_to_row(prtg_sensor: PRTGSensor, smartsheet_sheet: SmartsheetSheet) -> SmartsheetRow:
    """
    Given a PRTG sensor object, convert the sensor's data into a Smartsheet row
    object.

    Args:
        prtg_sensor (PRTGSensor): The sensor data we want to convert.
        smartsheet_sheet (SmartsheetSheet): The 
            Smartsheet we want to insert the row into.

    Returns:
        SmartsheetRow: The Smartsheet row object 
            containing the sensor's data.
    """

    # Initialize the row object we will be returning.
    sensor_row = smartsheet.models.Row()
    sensor_row.to_top = True

    # Initialize the cell with the sensor's status.
    status_cell = SmartsheetCell()
    status_cell.column_id = smartsheet_sheet.columns[0].id
    status_cell.value = prtg_sensor.status

    # Initialize the cell with the sensor's occurrance timestamp.
    occurred_cell = SmartsheetCell()
    occurred_cell.column_id = smartsheet_sheet.columns[1].id
    occurred_cell.value = prtg_sensor.downtime_since

    # Initialize the cell with the sensor's name.
    name_cell = SmartsheetCell()
    name_cell.column_id = smartsheet_sheet.columns[2].id
    name_cell.value = prtg_sensor.name

    # Initialize the cell with the sensor's probe / group / device.
    probe_group_device_cell = SmartsheetCell()
    probe_group_device_cell.column_id = smartsheet_sheet.columns[3].id
    probe_group_device_cell.value = prtg_sensor.probe + ' > ' + \
        prtg_sensor.group + ' > ' + prtg_sensor.device

    # Initialize the cell with the sensor's message.
    message_cell = SmartsheetCell()
    message_cell.column_id = smartsheet_sheet.columns[4].id
    message_cell.value = prtg_sensor.message

    # Update the row object with the all the cell objects.
    sensor_row.cells.append(status_cell)
    sensor_row.cells.append(occurred_cell)
    sensor_row.cells.append(name_cell)
    sensor_row.cells.append(probe_group_device_cell)
    sensor_row.cells.append(message_cell)

    # Return the row.
    return sensor_row


def get_alerting_prtg_sensors(prtg_instances: list[dict]) -> list[PRTGSensor]:
    """
    Return all non-online sensors from all provided PRTG instances with their
    respective credentials across all probes.

    Args:
        prtg_instances_data(list[dict]): A list of all PRTG instances data for 
            this customer.

    Returns:
        list[PRTGSensor]: A list of hard-typed PRTG sensor objects.
    """

    logger.info('Gathering PRTG sensor data...')

    # Get the raw sensors from each PRTG instance.
    all_prtg_sensors = list[PRTGSensor]()
    for prtg_instance_data in prtg_instances:
        # Check if we are using a default PRTG instance.
        if prtg_instance_data['url'] == PRTG_01_USE_DEFAULTS_KEYWORD:
            full_prtg_url = f'{PRTG_01_DEFAULT_INSTANCE_URL}/api/table.xml'
            prtg_api_key = PRTG_01_DEFAULT_API_KEY
        elif prtg_instance_data['url'] == PRTG_02_USE_DEFAULTS_KEYWORD:
            full_prtg_url = f'{PRTG_02_DEFAULT_INSTANCE_URL}/api/table.xml'
            prtg_api_key = PRTG_02_DEFAULT_API_KEY
        else:
            full_prtg_url = f'{prtg_instance_data['url']}/api/table.xml'
            prtg_api_key = prtg_instance_data['api_key']
            
        # Create the parameters for the PRTG API payload.
        prtg_api_parameters = {
                'content': 'sensors',
                'columns': 'name,parentid,downtimesince,status,' \
                           'probe,group,device,message',
                'filter_status': '@neq(3)',
                'output': 'json',
                'count': str(PRTG_MAX_RESPONSE_LIMIT),
                'apitoken': prtg_api_key
        }
        
        # Check if we need to add any filters to the probe.
        if len(prtg_instance_data['probe_substrings']) != 0:
            prtg_api_parameters['filter_probe'] = [f'@sub({probe_substring})' for probe_substring in prtg_instance_data['probe_substrings']]
            
        # Send the request to PRTG.
        prtg_raw_sensors_resp = requests.get(
            url=full_prtg_url,
            params=prtg_api_parameters
        )
        
        # Extract just the sensors from the response.
        prtg_raw_sensors = prtg_raw_sensors_resp.json()['sensors']

        # Convert all the raw PRTG sensor dictionaries to hard-typed PRTG sensor objects.
        prtg_sensors = list[PRTGSensor]()
        for prtg_raw_sensor in prtg_raw_sensors:
            prtg_sensor = PRTGSensor(prtg_raw_sensor['name'], prtg_raw_sensor['parentid'],
                                     prtg_raw_sensor['downtimesince'], prtg_raw_sensor['status'],
                                     prtg_raw_sensor['probe'], prtg_raw_sensor['group'],
                                     prtg_raw_sensor['device'], prtg_raw_sensor['message_raw'])
            prtg_sensors.append(prtg_sensor)

        # Add this PRTG instance's sensors to the customer's global sensor
        # list.
        all_prtg_sensors.extend(prtg_sensors)

    # Return all the PRTG sensor data.
    logger.info('PRTG sensor data gathered!')
    if len(all_prtg_sensors) > SMARTSHEET_MAX_DASHBOARD_ROW_COUNT:
        return all_prtg_sensors[:SMARTSHEET_MAX_DASHBOARD_ROW_COUNT]
    
    return all_prtg_sensors
    

def convert_prtg_sensors_to_smartsheet_rows(prtg_sensors: list[PRTGSensor], smartsheet_sheet: SmartsheetSheet) -> list[SmartsheetRow]:
    """
    Given a list of PRTG sensors and a desired Smartsheet sheet object, convert
    the list of sensors to a list of Smartsheet row objects and return the list
    of rows.

    Args:
        prtg_sensors (list[PRTGSensor]): The list of PRTG sensors to convert to
            Smartsheet rows.
        smartsheet_sheet (SmartsheetSheet): The
            desired Smartsheet the sensors should go into.
    
    Returns:
        list[SmartsheetRow]: The list of rows of the
            converted sensors.
    """
    
    # For each sensor, convert it into a Smartsheet row and add it to the
    # returning list of Smartsheet rows.
    all_sensor_rows = list[SmartsheetRow]()
    for prtg_sensor in prtg_sensors:
        prtg_sensor_row = prtg_sensor_to_row(prtg_sensor, smartsheet_sheet)
        all_sensor_rows.append(prtg_sensor_row)

    # Return all the sensor rows.
    return all_sensor_rows


def put_prtg_sensor_data_into_smartsheet(customer_config: dict) -> None:
    """
    Given a customer configuration, get the current non-online PRTG sensor data
    and push it into a Smartsheet.

    Args:
        customer_config (dict): The customer's configuration.
    """
    
    # Check if there are no PRTG instance URLs in the config.
    if len(customer_config['prtg_instances']) == 0:
        logger.info(f'No PRTG instances set for {customer_config['customer_name']}!')
        return

    # Get the current alerting PRTG sensors for this customer.
    current_alerting_prtg_sensors = get_alerting_prtg_sensors(
        customer_config['prtg_instances']
    )

    # Get a reference to this customer's PRTG sensor Smartsheet.
    prtg_smartsheet = SMARTSHEET_CLIENT.Sheets.get_sheet(customer_config['smartsheet_sheet_ids']['prtg_alerts'])

    # Convert the sensors to Smartsheet rows.
    current_alerting_prtg_sensors_rows = convert_prtg_sensors_to_smartsheet_rows(current_alerting_prtg_sensors, prtg_smartsheet)

    # Clear the Smartsheet before pushing the fresh data.
    clear_smartsheet(prtg_smartsheet)

    # Add all the rows to the Smartsheet.
    add_rows_to_smartsheet(prtg_smartsheet, current_alerting_prtg_sensors_rows)


def put_customer_data_into_smartsheets(customer_config: dict) -> None:
    """
    Given a customer configuration, get their quarterly Opsgenie alerts,
    quarterly ServiceNow tickets, and the current non-online PRTG sensor data
    and push it into their own respective Smartsheets.

    Args:
        customer_config (dict): The customer's configuration.
    """
    
    # Push this customer's Opsgenie alert data into a Smartsheet.
    put_opsgenie_data_into_smartsheet(customer_config)

    # Push this customer's ServiceNow tickets into a Smartsheet.
    put_servicenow_data_into_smartsheet(customer_config)

    # Push this customer's current PRTG sensor alerts into a Smartsheet.
    put_prtg_sensor_data_into_smartsheet(customer_config)


def main():
    """
    Runs the Quarterly Business Report automation!
    """

    logger.info('Beginning QBR automation...')

    # Push all customer alert and ticket data into their respective Smartsheets.
    for customer_config in CUSTOMER_CONFIGS:
        logger.info(f'Beginning QBR automation for "{customer_config['customer_name']}"...')

        # Push all this customer's new data into their respective Smartsheets.
        put_customer_data_into_smartsheets(customer_config)

        logger.info(f'Completed QBR automation for "{customer_config['customer_name']}"!')
    
    logger.info('QBR automation completed successfully!')


if __name__ == "__main__":
    main()
