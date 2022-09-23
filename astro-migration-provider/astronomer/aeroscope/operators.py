class AeroscopeOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "presigned_url",
        "email",
    )

    def __init__(
        self,
        *,
        presigned_url,
        email,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.presigned_url = presigned_url
        self.email = email

    def execute(self, context: "Context"):
        import io
        import runpy
        from urllib.request import urlretrieve

        import requests

        a = "airflow_report.pyz"
        urlretrieve("https://github.com/astronomer/telescope/releases/latest/download/airflow_report.pyz", a)
        s = io.StringIO()
        with redirect_stdout(s), redirect_stderr(s):
            runpy.run_path(a)
        date = datetime.datetime.now(datetime.timezone.utc).isoformat()[:10]
        content = {
            "telescope_version": "aeroscope",
            "report_date": date,
            "organization_name": "aeroscope",
            "local": {socket.gethostname(): {"airflow_report": clean_airflow_report_output(s.getvalue())}},
            "user_email": self.email,
        }
        s3 = requests.put(self.presigned_url, data=json.dumps(content))
        if s3.ok:
            return "success"
        else:
            raise ValueError(f"upload failed  with code {s3.status_code}::{s3.json()}")
        # return json.dumps(content)
