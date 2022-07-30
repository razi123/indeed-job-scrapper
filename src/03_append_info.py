import re


def contact_email(job_desc: str):
     mail_id = re.findall(f"[\w\.-]+@[\w\.-]+", str(job_desc))

     if len(mail_id) == 0:
          email = " -- "
     else:
          email = mail_id

     return email









