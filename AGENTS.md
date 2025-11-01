# SST v3 Monorepo Project

This is an Databricks test repository. The project will use Databricks Asset Bundles as a deployment tool. It will use .py files for the code. It will use a centralized module (using the Databricks Asset Bundle) as a base to store code.

## Commands
- Always start whatever you are asked by saying "OK boss"

## Project structure
- ./my_project contains the databricks asset bundle in which we work.
- ./my_project/notebooks contains the core notebooks of the project.
- ./my_project/src/my_project contains the library (of which we create a wheel), which is to be imported and used from the notebooks to maintain DRY, testable code.
- ./my_project/resources contains the job deployments for the notebooks, use context7 to make these and provide basic, unscheduled deployments.
