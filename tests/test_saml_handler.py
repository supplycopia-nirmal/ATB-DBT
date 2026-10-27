import unittest
import base64
from pathlib import Path
import xml.etree.ElementTree as ET

from autonomous_dbt_app.core.saml_handler import SAMLHandler


class TestSAMLHandler(unittest.TestCase):
    def setUp(self):
        self.handler = SAMLHandler(
            sp_entity_id="ab02a3b2-878b-411b-8c9c-7c42a2229c69",
            sp_acs_url="https://atb-dbt.supplycopia.com/saml2/acs",
            idp_entity_id="https://sts.windows.net/ae81ad54-d58a-4097-9fc7-98a2954a7a6d/",
            idp_sso_url="https://login.microsoftonline.com/ae81ad54-d58a-4097-9fc7-98a2954a7a6d/saml2",
            session_secret="test_secret_key_12345"
        )

    def test_generate_authn_request(self):
        redirect_url, req_id = self.handler.generate_authn_request(relay_state="/explorer")
        self.assertTrue(req_id.startswith("_"))
        self.assertIn("https://login.microsoftonline.com", redirect_url)
        self.assertIn("SAMLRequest=", redirect_url)
        self.assertIn("RelayState=%2Fexplorer", redirect_url)

    def test_session_token_lifecycle(self):
        user_info = {
            "email": "cfo@supplycopia.com",
            "name": "Jane Doe",
            "roles": ["Executive"],
            "idp": "Microsoft Entra ID"
        }
        token = self.handler.create_session_token(user_info, expires_in_seconds=3600)
        self.assertIsInstance(token, str)
        self.assertEqual(len(token.split('.')), 3)

        payload = self.handler.verify_session_token(token)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["sub"], "cfo@supplycopia.com")
        self.assertEqual(payload["name"], "Jane Doe")
        self.assertEqual(payload["roles"], ["Executive"])

        # Tampered token verification
        tampered_token = token[:-4] + "ABCD"
        self.assertIsNone(self.handler.verify_session_token(tampered_token))

    def test_process_saml_response_success(self):
        sample_xml = """<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
                        xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
                        ID="resp_001" Version="2.0">
          <samlp:Status>
            <samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>
          </samlp:Status>
          <saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" ID="as_001" Version="2.0">
            <saml:Subject>
              <saml:NameID>analyst@supplycopia.com</saml:NameID>
            </saml:Subject>
            <saml:AttributeStatement>
              <saml:Attribute Name="http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name">
                <saml:AttributeValue>analyst@supplycopia.com</saml:AttributeValue>
              </saml:Attribute>
              <saml:Attribute Name="http://schemas.microsoft.com/identity/claims/displayname">
                <saml:AttributeValue>Supply Chain Analyst</saml:AttributeValue>
              </saml:Attribute>
              <saml:Attribute Name="http://schemas.microsoft.com/ws/2008/06/identity/claims/role">
                <saml:AttributeValue>Data &amp; Analytics</saml:AttributeValue>
              </saml:Attribute>
            </saml:AttributeStatement>
          </saml:Assertion>
        </samlp:Response>"""
        b64 = base64.b64encode(sample_xml.encode('utf-8')).decode('ascii')
        result = self.handler.process_saml_response(b64)
        self.assertTrue(result["authenticated"])
        self.assertEqual(result["email"], "analyst@supplycopia.com")
        self.assertEqual(result["name"], "Supply Chain Analyst")

    def test_process_saml_response_failure(self):
        fail_xml = """<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
                      ID="resp_fail" Version="2.0">
          <samlp:Status>
            <samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Responder"/>
            <samlp:StatusMessage>User does not have access</samlp:StatusMessage>
          </samlp:Status>
        </samlp:Response>"""
        b64 = base64.b64encode(fail_xml.encode('utf-8')).decode('ascii')
        result = self.handler.process_saml_response(b64)
        self.assertFalse(result["authenticated"])
        self.assertIn("User does not have access", result["error"])


if __name__ == "__main__":
    unittest.main()
