import os
import json
import logging
import requests
from authorization import OAuth2Bearer
from abc import ABC, abstractmethod
from kafka import KafkaProducer

BEARER_TOKEN = os.environ.get("BEARER_TOKEN")


class Stream(ABC):

    def __init__(self):
        self.authentication = OAuth2Bearer(BEARER_TOKEN)

    @abstractmethod
    def endpoint(self):
        pass

    def connect_to_endpoint(self):
        endpoint = self.endpoint
        response = requests.get(
            endpoint, auth=self.authentication, stream=True)

        if response.status_code != 200:
            raise Exception("Request returned an error: %s %s" %
                            (response.status_code, response.text))

        return response

    def flush_tweets(self, response):
        for response_line in response.iter_lines():
            if response_line:
                yield response_line


class SampledStream(Stream):
    def __init__(self, parameters: dict = None) -> None:
        """
        Args:
            parameters(dict, optional): Customize your request using the API reference.
            All references available at - https: // developer.twitter.com/en/docs/twitter-api/tweets/sampled-stream/api-reference/get-tweets-sample-stream
            Defaults to None.
        """
        super().__init__()
        if parameters is not None:
            self.custom_parameters = "&".join(
                key.replace(' ', '').lower() + "=" + value.replace(' ', '').lower() for key, value in parameters.items())
        else:
            self.custom_parameters = None

    @property
    def endpoint(self) -> str:
        endpoint = "https://api.twitter.com/2/tweets/sample/stream"
        if self.custom_parameters is not None:
            return endpoint + "?" + self.custom_parameters
        else:
            return endpoint


class FilteredStream(Stream):

    def endpoint(self, rules=True) -> str:
        if rules:
            return "https://api.twitter.com/2/tweets/search/stream/rules"
        else:
            return "https://api.twitter.com/2/tweets/search/stream"

    def read_rules(self):
        response = requests.get(self.endpoint, auth=self.authentication)
        if response.status_code != 200:
            raise Exception("Cannot download rules (HTTP %s) : %s" %
                            response.status_code, response.text)
        else:
            print(json.dumps(response.json(), indent=4, ensure_ascii=False))

    def add_rules(self, rules: list) -> None:
        if isinstance(rules, list):

            for _rule in rules:
                if isinstance(_rule, dict):
                    continue
                else:
                    raise Exception(
                        "Type Error: must be dict, not %s" % type(_rule).__name__)

            payload = {"add": rules}
            response = requests.post(
                self.endpoint(), json=payload, auth=self.authentication)

            if response.status_code != 201:
                raise Exception("Cannot add rules (HTTP %s): %s" %
                                (response.status_code, response.text))
            print(json.dumps(response.json(), indent=4, ensure_ascii=False))

        else:
            raise Exception("Type Error: must be list, not %s" %
                            type(rules).__name__)

    def _rule_removal(self, rule_ids: list):
        payload = {"delete": {"ids": rule_ids}}
        response = requests.post(
            self.endpoint(), json=payload, auth=self.authentication)
        if response.status_code != 201:
            raise Exception("Cannot remove rules (HTTP %s): %s" %
                            (response.status_code, response.text))
        print(json.dumps(response.json(), indent=4,
              sort_keys=True, ensure_ascii=False))

    def remove_all_rules(self, rules: dict) -> None:
        if rules is None or "data" not in rules:
            return None
        rule_ids = [rule["id"] for rule in rules["data"]]
        self._rule_removal(rule_ids=rule_ids)

    def remove_rule(self, rule_ids: list) -> None:
        if rule_ids is None:
            return None
        self._rule_removal(rule_ids=rule_ids)


if __name__ == "__main__":
    # Sample stream download
    stream = SampledStream()
    response = stream.connect_to_endpoint()
    print(response.status_code)
