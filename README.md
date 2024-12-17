# Quarterly Business Report Automation

## Summary
Gathers customer quarterly Opsgenie alerts, ServiceNow tickets, and currently
alerting PRTG sensors and pushes the raw data into several Smartsheets.

_Note: If you have any questions or comments you can always use GitHub
discussions, or email me at farinaanthony96@gmail.com._

#### Why
Having Opsgenie, ServiceNow, and PRTG data into Smartsheets enables our team to
create custom dashboards based off customer data. This makes the process of
presenting quarterly data more streamlined to help save PMs time.

## Requirements
- Python 3.12+
- loguru
- opsgenie_sdk
- pysnow
- python-dotenv
- python-magic (use "python-magic-bin" if running on a Windows system)
- requests
- smartsheet-python-sdk

## Usage
- Edit the example environment file with relevant Opsgenie, PRTG, ServiceNow,
  Smartsheet, and customer configuration information.

- Simply run the script using Python:
  `python quarterly_business_report_automation.py`

## Compatibility
Should be able to run on any machine with a Python interpreter. This script
was only tested on a Windows machine running Python 3.12.2.

## Disclaimer
The code provided in this project is an open source example and should not
be treated as an officially supported product. Use at your own risk. If you
encounter any problems, please log an
[issue](https://github.com/CC-Digital-Innovation/QBR-Automation/issues).

## Contributing
1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request ツ

## History
-  version 1.2.1 - 2024/12/17
    - Added support for multiple PRTG instances for each customer
    - Added support for timezones to trigger this script more consistently


-  version 1.2.0 - 2024/10/29
    - Capped data rows to 2,500 for Smartsheet so we can use dashboards
    - Added support for customers in ServiceNow that have multiple names
    - Added support for customers that don't use PRTG


-  version 1.0.1 - 2024/09/26
    - Put MVP into pipeline
    - More robust Opsgenie tagging recognition


-  version 1.0.0 - 2024/08/15
    - MVP initial release

## Credits
Anthony Farina <<farinaanthony96@gmail.com>>