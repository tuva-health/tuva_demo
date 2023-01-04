[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.3.x&color=orange)

# Tuva Claims Demo

## 🧰 What does this project do?

The Tuva Claims Demo provides a quick and easy way to run the Tuva Project with synthetic demo data. 
By default, the project will run with built-in sample data of 100 patients loaded as seeds.
If you want to run the demo project with the full demo data from a data share, please follow the instructions below under "Getting Started".

To setup the Tuva Project with your own claims data or to better understand what the Tuva Project does, please review the ReadMe in [The Tuva Project](https://github.com/tuva-health/the_tuva_project) package for a detailed walkthrough and setup.

For information on data models and to view the entire DAG check out our dbt [Docs](https://tuva-health.github.io/tuva_claims_demo/#!/overview).

## 🔌 Database Support

- Redshift
- Snowflake

## ✅ How to get started

### Pre-requisites
1. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse).
2. You have created a database for the output of this project to be written in your data warehouse.

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

### Getting Started
Complete the following steps to configure the project to run in your environment.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment.
2. Update the dbt_project.yml file:
   1. Add the dbt profile connected to your data warehouse.
   2. Update the variable "tuva_database" to use the new database you created for this project.
3. Run `dbt deps` to install the package. 
4. Run `dbt build` to run the entire project with the built-in sample data.

#### Full Demo Data
Complete the following steps to configure the project to run the full demo data from the Snowflake Marketplace.
You can learn more about how to access Snowflake Marketplace listings [here](https://other-docs.snowflake.com/en/collaboration/consumer-listings-access.html#accessing-listings-on-the-marketplace). 

1. Go to the Snowflake Marketplace [listing](https://app.snowflake.com/marketplace/listing/GZT0ZS2I9BQ/tuva-health-tuva-project-claims-demo).
2. Select Get.
3. Specify a database name for the data in the listing.
4. (Optional) Add roles to grant access to the database created from the listing.
5. Update the dbt_project.yml file:
   1. Update the variable "tuva_database" to the database specified in step 3.
   2. Set the variable "full_data_override" to `true`.
6. Run `dbt deps` to install the package (if you have not already done this).
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
