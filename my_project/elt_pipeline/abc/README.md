Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
How to run dbt:
```
cd to the /abc 
export DBT_PROFILES_DIR="$(pwd)"
dbt debug
dbt run
```
To see the ui :
``` 
cd to the /abc
dbt docs generate \ dbt docs serve --port 8081
```