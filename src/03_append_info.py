import datetime
# which website
# job role
# from which company
# contact info (HR Name)
# HR Email

import re

# Date, Source website, Role, Company,
from pyspark.sql.functions import DataFrame


def contact_email(job_desc: str):
     #mail_id = re.findall(f"(?<=\s)[A-Za-z0-9\-\.\@]+(?=\.[com|de]+)", str(job_desc))

     mail_id = re.findall(f"[\w\.-]+@[\w\.-]+", str(job_desc))

     if len(mail_id) == 0:
          email = " -- "
     else:
          email = mail_id

     return email









