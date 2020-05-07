import boto3
import json
import logging
from typing import Any
from robot.libraries.BuiltIn import BuiltIn, RobotNotRunningError
from RPA.RobotLogListener import RobotLogListener

try:
    BuiltIn().import_library("RPA.RobotLogListener")
except RobotNotRunningError:
    pass

DEFAULT_REGION = "eu-west-1"


class AWS:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.clients = {}
        listener = RobotLogListener()
        listener.register_protected_keywords(
            ["set_textract_client", "set_comprehend_client"]
        )

    def _set_service(self, service_name: str = None, client: Any = None):
        # some handle on overwriting client ?!
        self.clients[service_name] = client

    def _init_client(self, service_name, aws_key_id, aws_key, region):
        client = boto3.client(
            service_name,
            region_name=region,
            aws_access_key_id=aws_key_id,
            aws_secret_access_key=aws_key,
        )
        self._set_service(service_name, client)

    def get_client_for_service(self, service_name: str = None):
        """[summary]

        :param service_name: [description], defaults to None
        :return: [description]
        """
        return (
            self.clients[service_name] if service_name in self.clients.keys() else None
        )

    def set_textract_client(self, aws_key_id, aws_key, region: str = DEFAULT_REGION):
        """[summary]

        :param aws_key_id: [description]
        :param aws_key: [description]
        :param region: [description], defaults to None
        """
        self._init_client("textract", aws_key_id, aws_key, region)

    def textract(self, image_file, json_file):
        """[summary]

        :param image_file: [description]
        :param json_file: [description]
        """
        client = self.get_client_for_service("textract")
        with open(image_file, "rb") as img:
            response = client.analyze_document(
                Document={"Bytes": img.read()}, FeatureTypes=["TABLES", "FORMS"]
            )
        with open(json_file, "w") as f:
            json.dump(response, f)

    def set_comprehend_client(self, aws_key_id, aws_key, region: str = DEFAULT_REGION):
        """[summary]

        :param aws_key_id: [description]
        :param aws_key: [description]
        :param region: [description], defaults to DEFAULT_REGION
        """
        self._init_client("comprehend", aws_key_id, aws_key, region)

    def comprehend_sentiment(self, text_file, json_file, lang="en"):
        """[summary]

        :param text_file: [description]
        :param json_file: [description]
        :param lang: [description], defaults to "en"
        """
        client = self.get_client_for_service("comprehend")
        with open(text_file, "r") as f:
            text = f.read()
        response = client.detect_sentiment(Text=text, LanguageCode=lang)
        with open(json_file, "w") as f:
            json.dump(response, f)
