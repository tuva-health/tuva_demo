[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.3&color=orange)

# Tuva Claims Demo

## 🧰 What does this project do?

The Tuva Project uses [dbt](https://www.getdbt.com/) to orchestrate and execute code inside your data warehouse. This is repository provides a demonstration of how the Tuva Project works with synthetic claims data. For a detailed overview of what the project does and how it works, check out our [Knowledge Base](https://thetuvaproject.com/docs/getting-started). 
For information on data models and to view the entire DAG check out our dbt [Docs](https://tuva-health.github.io/tuva_claims_demo/#!/overview?g_v=1).

## 🔌 Database Support

- Snowflake

## ✅ How to get started

### Pre-requisites
1. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse).
2. You have created a database called `tuva_claims_demo` in your data warehouse.

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

### Getting Started
Complete the following steps to configure the project to run in your environment.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment
2. Update the dbt_project.yml file to use the dbt profile connected to your data warehouse.
3. Run the `dbt build` command.

## 🙋🏻‍♀️ **How is this project maintained and can I contribute?**

### Project Maintenance

The Tuva Project team maintaining this project **only** maintains the latest version of the project. 
We highly recommend you stay consistent with the latest version.

### Contributions

Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached on our Slack community.

## 🤝 Community

Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
