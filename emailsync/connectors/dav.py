"""Minimal CardDAV and CalDAV client implemented with HTTP and XML."""

import datetime as dt
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit

import httpx

from emailsync.errors import AuthError, PermanentError, ThrottledError, wrap
from emailsync.oauth import OAuthBearerAuth, StaticBearerAuth
from emailsync.options import Endpoint

DAV = "DAV:"
CARDDAV = "urn:ietf:params:xml:ns:carddav"
CALDAV = "urn:ietf:params:xml:ns:caldav"
_NS = {"d": DAV, "card": CARDDAV, "cal": CALDAV}
_REDIRECTS = {301, 302, 303, 307, 308}


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _safe_url(url: str) -> str:
    parts = urlsplit(url)
    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parts.port:
        host += f":{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))


def _origin(url: str) -> tuple[str, str, int | None]:
    parts = urlsplit(url)
    return parts.scheme.lower(), (parts.hostname or "").lower(), parts.port


def _auth_for(url: str, base: str, auth):
    if not auth:
        return None
    target, original = urlsplit(url), urlsplit(base)
    same_host = (target.hostname or "").lower() == (original.hostname or "").lower()
    same_origin = _origin(url) == _origin(base)
    secure_upgrade = original.scheme.lower() == "http" and target.scheme.lower() == "https"
    return auth if same_host and (same_origin or secure_upgrade) else None


def _xml(tag: str, *children: ET.Element) -> ET.Element:
    node = ET.Element(tag)
    node.extend(children)
    return node


def _propfind_xml(kind: str) -> bytes:
    root = ET.Element(f"{{{DAV}}}propfind")
    prop = ET.SubElement(root, f"{{{DAV}}}prop")
    for name in ("resourcetype", "displayname", "current-user-principal"):
        ET.SubElement(prop, f"{{{DAV}}}{name}")
    ns = CARDDAV if kind == "carddav" else CALDAV
    ET.SubElement(prop, f"{{{ns}}}{'addressbook-home-set' if kind == 'carddav' else 'calendar-home-set'}")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


class DavClient:
    def __init__(self, endpoint: Endpoint, kind: str = "carddav", timeout: float = 60.0):
        if kind not in ("carddav", "caldav"):
            raise ValueError("kind must be carddav or caldav")
        self.endpoint = endpoint
        self.kind = kind
        base = endpoint.url.strip()
        if not base.startswith(("http://", "https://")):
            base = "https://" + base
        self.base = _safe_url(base.rstrip("/") + "/")
        if endpoint.token_provider is not None:
            self.auth = OAuthBearerAuth(endpoint.token_provider)
        elif endpoint.token:
            self.auth = StaticBearerAuth(endpoint.token)
        else:
            self.auth = (
                (endpoint.username, endpoint.password) if endpoint.username or endpoint.password else None
            )
        self.http = httpx.Client(
            verify=not endpoint.insecure_tls,
            timeout=httpx.Timeout(timeout, connect=20.0),
            follow_redirects=False,
            headers={"User-Agent": "email-sync/1.0"},
        )

    def close(self) -> None:
        self.http.close()

    def _request(self, method: str, url: str, *, headers=None, content=None):
        url = _safe_url(url)
        for _ in range(7):
            try:
                response = self.http.request(
                    method,
                    url,
                    headers=headers,
                    content=content,
                    auth=_auth_for(url, self.base, self.auth),
                )
            except Exception as exc:
                raise wrap(exc) from exc
            if response.status_code in (401, 403):
                raise AuthError(f"DAV login failed (HTTP {response.status_code})")
            if response.status_code not in _REDIRECTS:
                if response.status_code == 412:
                    return response
                if response.status_code == 429:
                    retry = response.headers.get("Retry-After", "")
                    try:
                        delay = max(0.0, float(retry))
                    except ValueError:
                        try:
                            retry_at = parsedate_to_datetime(retry)
                            delay = max(0.0, (retry_at - dt.datetime.now(dt.UTC)).total_seconds())
                        except (TypeError, ValueError):
                            delay = 300.0
                    raise ThrottledError("DAV server rate limited the request", delay)
                if response.status_code >= 400:
                    try:
                        response.raise_for_status()
                    except Exception as exc:
                        raise wrap(exc) from exc
                return response
            location = response.headers.get("Location")
            if not location:
                return response
            url = _safe_url(urljoin(url, location))
            if response.status_code == 303:
                method, content = "GET", None
        raise RuntimeError("Too many DAV redirects")

    def _propfind(self, url: str, depth: str = "0", body: bytes | None = None):
        return self._request(
            "PROPFIND",
            url,
            headers={"Depth": depth, "Content-Type": "application/xml; charset=utf-8"},
            content=body if body is not None else _propfind_xml(self.kind),
        )

    def _responses(self, response) -> list[tuple[str, ET.Element]]:
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as exc:
            raise PermanentError("DAV server returned invalid XML") from exc
        out = []
        for item in root.iter():
            if _local(item.tag) != "response":
                continue
            href = item.find("d:href", _NS)
            if href is not None and href.text:
                out.append((urljoin(str(response.url), href.text), item))
        return out

    @staticmethod
    def _properties(response_node: ET.Element) -> dict[str, ET.Element]:
        properties = {}
        for propstat in response_node:
            if _local(propstat.tag) != "propstat":
                continue
            status = propstat.find("d:status", _NS)
            if status is not None:
                parts = (status.text or "").split()
                if len(parts) < 2 or not parts[1].startswith("2"):
                    continue
            prop = propstat.find("d:prop", _NS)
            if prop is not None:
                properties.update({_local(child.tag): child for child in prop})
        return properties

    def _collection_type(self, node: ET.Element) -> str | None:
        props = self._properties(node)
        resource = props.get("resourcetype")
        if resource is None:
            return None
        expected = "addressbook" if self.kind == "carddav" else "calendar"
        return expected if any(_local(child.tag) == expected for child in resource) else None

    def _home(self, url: str, node: ET.Element) -> str | None:
        props = self._properties(node)
        key = "addressbook-home-set" if self.kind == "carddav" else "calendar-home-set"
        if key not in props:
            return None
        href = next(
            (child.text for child in props[key].iter() if _local(child.tag) == "href" and child.text), None
        )
        return urljoin(url, href) if href else None

    def _principal(self, url: str, node: ET.Element) -> str | None:
        props = self._properties(node)
        principal = props.get("current-user-principal")
        if principal is None:
            return None
        href = next(
            (child.text for child in principal.iter() if _local(child.tag) == "href" and child.text), None
        )
        return urljoin(url, href) if href else None

    def discover(self) -> tuple[str, list[dict]]:
        candidates = [self.base]
        try:
            first = self._propfind(self.base)
            responses = self._responses(first)
            current = responses[0][1] if responses else ET.Element("empty")
        except PermanentError:
            current = ET.Element("empty")
        if self._collection_type(current):
            return self.base, [self._collection(self.base, current)]
        home = self._home(self.base, current)
        principal = self._principal(self.base, current)
        if not home and principal:
            try:
                principal_responses = self._responses(self._propfind(principal))
            except PermanentError:
                principal_responses = []
            if principal_responses:
                home = self._home(principal, principal_responses[0][1])
        if home:
            candidates = [home]
        else:
            scheme_host = urlsplit(self.base)
            well_known = urlunsplit(
                (scheme_host.scheme, scheme_host.netloc, f"/.well-known/{self.kind}", "", "")
            )
            wk_response = self._propfind(well_known)
            wk_url = _safe_url(str(wk_response.url))
            wk_responses = self._responses(wk_response)
            wk_node = wk_responses[0][1] if wk_responses else ET.Element("empty")
            if self._collection_type(wk_node):
                return wk_url, [self._collection(wk_url, wk_node)]
            home = self._home(wk_url, wk_node)
            principal = self._principal(wk_url, wk_node)
            if not home and principal:
                try:
                    principal_responses = self._responses(self._propfind(principal))
                except PermanentError:
                    principal_responses = []
                if principal_responses:
                    home = self._home(principal, principal_responses[0][1])
            if home:
                candidates = [home]
            else:
                raise RuntimeError(f"DAV server did not advertise a {self.kind} home set")
        home_url = _safe_url(candidates[0])
        listing = self._propfind(home_url, depth="1")
        collections = []
        for href, node in self._responses(listing):
            if self._collection_type(node):
                collections.append(self._collection(href, node))
        return home_url, collections

    @staticmethod
    def _collection(href: str, node: ET.Element) -> dict:
        props = DavClient._properties(node)
        display = props.get("displayname")
        name = (
            display.text.strip()
            if display is not None and display.text
            else unquote(urlsplit(href).path.rstrip("/").split("/")[-1])
        )
        return {"href": _safe_url(href), "displayname": name}

    def list_objects(self, collection: dict | str) -> list[tuple[str, str | None, str]]:
        href = collection["href"] if isinstance(collection, dict) else collection
        request = ET.Element(f"{{{DAV}}}propfind")
        prop = ET.SubElement(request, f"{{{DAV}}}prop")
        ET.SubElement(prop, f"{{{DAV}}}getetag")
        response = self._propfind(
            href, depth="1", body=ET.tostring(request, encoding="utf-8", xml_declaration=True)
        )
        entries = []
        for obj_href, node in self._responses(response):
            if _safe_url(obj_href).rstrip("/") == _safe_url(href).rstrip("/"):
                continue
            props = self._properties(node)
            etag = props.get("getetag")
            entries.append((obj_href, etag.text if etag is not None else None))
        object_refs = list(entries)
        objects = []
        data_tag = "address-data" if self.kind == "carddav" else "calendar-data"
        ns = CARDDAV if self.kind == "carddav" else CALDAV
        multi = ET.Element(
            f"{{{ns}}}{'addressbook-multiget' if self.kind == 'carddav' else 'calendar-multiget'}"
        )
        prop = ET.SubElement(multi, f"{{{DAV}}}prop")
        ET.SubElement(prop, f"{{{DAV}}}getetag")
        ET.SubElement(prop, f"{{{ns}}}{data_tag}")
        for start in range(0, len(entries), 100):
            body = ET.Element(multi.tag)
            body.extend(list(multi))
            for obj_href, _etag in object_refs[start : start + 100]:
                ET.SubElement(body, f"{{{DAV}}}href").text = obj_href
            result = self._request(
                "REPORT",
                href,
                headers={"Depth": "1", "Content-Type": "application/xml; charset=utf-8"},
                content=ET.tostring(body, encoding="utf-8", xml_declaration=True),
            )
            for obj_href, node in self._responses(result):
                props = self._properties(node)
                data = props.get(data_tag)
                etag = props.get("getetag")
                if data is not None and data.text is not None:
                    objects.append((obj_href, etag.text if etag is not None else None, data.text))
        return objects

    def put(self, collection: dict | str, filename: str, data: bytes) -> bool:
        base = collection["href"] if isinstance(collection, dict) else collection
        url = urljoin(base.rstrip("/") + "/", quote(filename, safe=""))
        response = self._request("PUT", url, headers={"If-None-Match": "*"}, content=data)
        if response.status_code == 412:
            return False
        if response.status_code not in (201, 204):
            response.raise_for_status()
        return True

    def create_collection(self, name: str) -> dict:
        base, collections = self.discover()
        if self.kind == "carddav":
            root = ET.Element(f"{{{DAV}}}mkcol")
            set_node = ET.SubElement(root, f"{{{DAV}}}set")
            prop = ET.SubElement(set_node, f"{{{DAV}}}prop")
            resource = ET.SubElement(prop, f"{{{DAV}}}resourcetype")
            ET.SubElement(resource, f"{{{DAV}}}collection")
            ET.SubElement(resource, f"{{{CARDDAV}}}addressbook")
            ET.SubElement(prop, f"{{{DAV}}}displayname").text = name
            method = "MKCOL"
        else:
            root = ET.Element(f"{{{CALDAV}}}mkcalendar")
            set_node = ET.SubElement(root, f"{{{DAV}}}set")
            prop = ET.SubElement(set_node, f"{{{DAV}}}prop")
            resource = ET.SubElement(prop, f"{{{DAV}}}resourcetype")
            ET.SubElement(resource, f"{{{DAV}}}collection")
            ET.SubElement(resource, f"{{{CALDAV}}}calendar")
            ET.SubElement(prop, f"{{{DAV}}}displayname").text = name
            method = "MKCALENDAR"
        url = urljoin(base.rstrip("/") + "/", quote(name, safe=""))
        response = self._request(
            method,
            url,
            headers={"Content-Type": "application/xml; charset=utf-8"},
            content=ET.tostring(root, encoding="utf-8", xml_declaration=True),
        )
        if response.status_code not in (200, 201, 204):
            response.raise_for_status()
        return {"href": url, "displayname": name}
