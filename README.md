[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.3.x&color=orange)

# Tuva Claims Demo

## 🧰 What does this project do?

The Tuva Claims Demo provides a quick and easy way to run the Tuva Project with synthetic demo data. 
By default, the project will run with built-in sample data of 100 patients loaded as dbt seeds.
If you want to run the demo project with the full demo data from a data share (available for free), please follow the instructions below under **"Running the Project with Full Demo Data"**.

To set up the Tuva Project with your own claims data or to better understand what the Tuva Project does, please review the ReadMe in [The Tuva Project](https://github.com/tuva-health/the_tuva_project) package for a detailed walkthrough and setup.

For information on data models and to view the entire DAG check out our dbt [Docs](https://tuva-health.github.io/tuva_claims_demo/#!/overview).

## 🔌 Database Support

- Redshift
- Snowflake

## ✅ How to get started

### Pre-requisites
1. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse). If you have not installed dbt, [here](https://docs.getdbt.com/dbt-cli/installation) are instructions for doing so.
2. You have created a database for the output of this project to be written in your data warehouse.

### Getting Started
Complete the following steps to configure the project to run in your environment.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment.
2. Update the `dbt_project.yml` file:
   1. Add the dbt profile connected to your data warehouse.
   2. Update the variable `tuva_database` to use the new database you created for this project.
3. Run `dbt deps` to install the package. 
4. Run `dbt build` to run the entire project with the built-in sample data.

### Running the Project with Full Demo Data
Complete the following additional steps to configure the project to run with the full demo data from one of the supported data shares.

#### *AWS Data Exchange (Redshift)*
The full demo data is available as a Redshift data product in the AWS Data Exchange. You can learn more about how to access the Data Exchange [here](https://docs.aws.amazon.com/data-exchange/latest/userguide/subscriber-getting-started.html).

1. Make sure you are in the US East (N. Virginia) region.
2. Go to the Tuva Project Claims Demo [product](https://us-west-1.console.aws.amazon.com/dataexchange/home?region=us-west-1#/products/prodview-nknghzaupuq5y) on the AWS Data Exchange.
3. Select *Continue to Subscribe*. Once you subscribe, it will take a few minutes for AWS to process the request.
4. Go to the Redshift Console and select your cluster. 
5. Go to the Datashare tab and select any database to view your subscriptions. 
6. Under *Subscriptions to AWS Data Exchange datashares*, select the `tuva_project_claims_demo` and choose *Create database from datashare*.
7. Specify a database name for the data in the listing.
8. Update the `dbt_project.yml` file:
   1. Update the variable `tuva_database` to the database specified in step 7.
   2. Set the variable `full_data_override` to **'true'**.
9. Run `dbt deps` to install the Tuva package (*you only need to do this once per local environment*).
10. Run `dbt build` to run the entire project.

#### *BigQuery*
The full demo data is available as a public dataset in BigQuery and can be queried directly.

1. Update the `dbt_project.yml` file:
   1. Update the variable `tuva_database` to your Google Cloud project id.
   2. Set the variable `full_data_override` to **'true'**. *(Note: This project includes logic to use the public dataset automatically when this variable is set to true.)*
2. Run `dbt deps` to install the Tuva package (*you only need to do this once per local environment*).
3. Run `dbt build` to run the entire project.


#### *Snowflake Marketplace*
The full demo data is available in the Snowflake Marketplace. You can learn more about how to access Snowflake Marketplace listings [here](https://other-docs.snowflake.com/en/collaboration/consumer-listings-access.html#accessing-listings-on-the-marketplace).

1. Go to the Tuva Project Claims Demo [listing](https://app.snowflake.com/marketplace/listing/GZT0ZS2I9BQ/tuva-health-tuva-project-claims-demo) on the Snowflake Marketplace.
2. Select Get.
3. Specify a database name for the data in the listing.
4. (Optional) Add roles to grant access to the database created from the listing.
5. Update the `dbt_project.yml` file:
   1. Update the variable `tuva_database` to the database specified in step 3.
   2. Set the variable `full_data_override` to **'true'**.
6. Run `dbt deps` to install the Tuva package (*you only need to do this once per local environment*).
7. Run `dbt build` to run the entire project.

## 🙋🏻‍♀️ **How is this project maintained and can I contribute?**

### Project Maintenance

The Tuva Project team maintaining this project **only** maintains the latest version of the project. 
We highly recommend you stay consistent with the latest version.

### Contributions

Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached on our Slack community.

## 🤝 Community

Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
