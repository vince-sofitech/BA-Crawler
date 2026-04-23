#!/usr/bin/env python3
"""
Robust DMCI leasing crawler.

This script crawls:
1) Properties listing pages
2) Property detail pages
3) Unit listing pages (and "View Unit" modal details)

It extracts structured leasing data and writes a deduplicated UTF-8 CSV.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag


HTTP_HEADERS = {
	"User-Agent": (
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
		"AppleWebKit/537.36 (KHTML, like Gecko) "
		"Chrome/124.0.0.0 Safari/537.36"
	)
}

REQUIRED_HEADERS = [
	"Property Name",
	"Unit Number",
	"Price Range",
	"Location",
	"Unit Finish",
	"Bedrooms",
	"Lease Type",
	"Parking",
	"Association Dues",
	"Pet Policy",
]


@dataclass
class CrawlerConfig:
	base_url: str = "https://leasing.dmcihomes.com"
	properties_path: str = "/properties/"
	output_csv: str = "dmci_leasing_units.csv"
	timeout: int = 25
	max_retries: int = 4
	rate_limit_seconds: float = 0.35
	max_pages: int = 20
	max_properties: Optional[int] = None
	use_selenium: bool = False


def clean_text(value: Optional[str]) -> str:
	if not value:
		return ""
	return re.sub(r"\s+", " ", value).strip()


def normalize_price(value: str) -> str:
	text = clean_text(value)
	if not text:
		return ""
	text = re.sub(r"^php\b", "PHP", text, flags=re.IGNORECASE)
	text = re.sub(r"^php\.", "PHP", text, flags=re.IGNORECASE)
	return text


def normalize_unit_finish(value: str) -> str:
	text = clean_text(value)
	lower = text.lower()
	if not lower:
		return ""
	if "bare unit with additional" in lower:
		return "Bare Unit With Additional"
	if "bare unit" in lower:
		return "Bare Unit"
	if "semi" in lower:
		return "Semi-Furnished"
	if "fully" in lower:
		return "Fully-Furnished"
	return text


def normalize_bedrooms(value: str) -> str:
	text = clean_text(value)
	lower = text.lower()
	if not lower:
		return ""
	if "studio" in lower:
		return "Studio"
	match = re.search(r"(\d+)\s*bedroom", lower)
	if match:
		return f"{match.group(1)} Bedroom"
	return text


def normalize_lease_type(value: str) -> str:
	text = clean_text(value)
	lower = text.lower()
	if not lower:
		return ""
	if "short" in lower:
		return "Short Term"
	if "long" in lower:
		return "Long Term"
	return text


def normalize_association_dues(value: str) -> str:
	text = clean_text(value)
	lower = text.lower()
	if not lower:
		return ""
	if "not included" in lower:
		return "Not Included"
	if "included" in lower:
		return "Included"
	return text


def normalize_pet_policy(value: str) -> str:
	text = clean_text(value)
	lower = text.lower()
	if not lower:
		return ""
	if "not allowed" in lower or lower.startswith("no"):
		return "Not Allowed"
	if "allowed" in lower or lower.startswith("yes"):
		return "Allowed"
	return text


def normalize_parking(value: str) -> str:
	text = clean_text(value)
	lower = text.lower()
	if not lower:
		return ""

	number_match = re.search(r"-?\d+(?:\.\d+)?", text)
	if number_match:
		try:
			return f"{float(number_match.group(0)):.2f}"
		except ValueError:
			return text

	if "not included" in lower or lower in {"none", "no", "n/a", "na"}:
		return "0.00"

	return text


def infer_unit_finish(text: str, unit_url: str) -> str:
	from_text = normalize_unit_finish(text)
	if from_text:
		return from_text

	path_parts = [p for p in urlparse(unit_url).path.split("/") if p]
	if "units" in path_parts:
		units_idx = path_parts.index("units")
		if units_idx + 1 < len(path_parts):
			candidate = path_parts[units_idx + 1].replace("-", " ")
			return normalize_unit_finish(candidate)
	return ""


def infer_bedrooms(text: str, unit_url: str) -> str:
	from_text = normalize_bedrooms(text)
	if from_text:
		return from_text

	path_parts = [p for p in urlparse(unit_url).path.split("/") if p]
	if "units" in path_parts:
		units_idx = path_parts.index("units")
		if units_idx + 2 < len(path_parts):
			candidate = path_parts[units_idx + 2].replace("-", " ")
			return normalize_bedrooms(candidate)
	return ""


def normalize_property_url(base_url: str, href: str) -> str:
	absolute = urljoin(base_url, href)
	parsed = urlparse(absolute)
	path = parsed.path
	if path and not path.endswith("/"):
		path += "/"
	cleaned = parsed._replace(path=path, query="", fragment="")
	return urlunparse(cleaned)


def normalize_page_url(base_url: str, href: str) -> str:
	absolute = urljoin(base_url, href)
	parsed = urlparse(absolute)
	cleaned = parsed._replace(fragment="")
	return urlunparse(cleaned)


def is_property_page_url(url: str) -> bool:
	parsed = urlparse(url)
	path = parsed.path
	if not path.startswith("/properties/"):
		return False
	if "/units/" in path:
		return False
	return path.rstrip("/") != "/properties"


def is_unit_page_url(url: str) -> bool:
	path = urlparse(url).path
	return path.startswith("/properties/") and "/units/" in path


def build_session() -> requests.Session:
	session = requests.Session()
	session.headers.update(HTTP_HEADERS)
	return session


def fetch_url(session: requests.Session, config: CrawlerConfig, url: str) -> str:
	last_error: Optional[Exception] = None

	for attempt in range(1, config.max_retries + 1):
		try:
			response = session.get(url, timeout=config.timeout)
			status = response.status_code

			if status in {429, 500, 502, 503, 504}:
				raise requests.HTTPError(f"Retryable status code: {status}")

			if status >= 400:
				logging.warning("Skipping %s due to HTTP %s", url, status)
				return ""

			return response.text
		except requests.RequestException as exc:
			last_error = exc
			if attempt >= config.max_retries:
				break

			backoff = min(2 ** (attempt - 1), 8) + random.uniform(0.1, 0.5)
			logging.warning(
				"Request failed (attempt %s/%s): %s | %s",
				attempt,
				config.max_retries,
				url,
				exc,
			)
			time.sleep(backoff)
		finally:
			if config.rate_limit_seconds > 0:
				time.sleep(config.rate_limit_seconds)

	logging.error("Failed to fetch %s: %s", url, last_error)
	return ""


def extract_property_links_from_listing(soup: BeautifulSoup, base_url: str) -> Set[str]:
	property_urls: Set[str] = set()

	lists: List[Tag] = []
	primary = soup.select_one("ul#available-property-list.available-property-list")
	if isinstance(primary, Tag):
		lists.append(primary)

	for ul in soup.select("ul.available-property-list"):
		if isinstance(ul, Tag):
			lists.append(ul)

	for ul in lists:
		for anchor in ul.select("a[href]"):
			href = anchor.get("href", "")
			if not href:
				continue
			absolute = normalize_property_url(base_url, href)
			if is_property_page_url(absolute):
				property_urls.add(absolute)

	if property_urls:
		return property_urls

	# Fallback if expected list is missing.
	for anchor in soup.select("a[href]"):
		href = anchor.get("href", "")
		if not href:
			continue
		absolute = normalize_property_url(base_url, href)
		if is_property_page_url(absolute):
			property_urls.add(absolute)

	return property_urls


def extract_pagination_urls(soup: BeautifulSoup, base_url: str) -> Set[str]:
	pages: Set[str] = set()
	for anchor in soup.select("a[href]"):
		href = anchor.get("href", "")
		if "page=" not in href:
			continue

		absolute = normalize_page_url(base_url, href)
		parsed = urlparse(absolute)
		if not parsed.path.startswith("/properties/"):
			continue
		if "page=" not in parsed.query.lower():
			continue
		pages.add(absolute)
	return pages


def fetch_property_urls_from_api(
	session: requests.Session, config: CrawlerConfig
) -> Set[str]:
	api_url = urljoin(config.base_url, "/api/properties")
	raw = fetch_url(session, config, api_url)
	if not raw:
		return set()

	try:
		payload = json.loads(raw)
	except json.JSONDecodeError:
		logging.warning("Could not decode JSON from %s", api_url)
		return set()

	items: List[Dict[str, object]] = []
	if isinstance(payload, list):
		items = [item for item in payload if isinstance(item, dict)]
	elif isinstance(payload, dict):
		for value in payload.values():
			if isinstance(value, dict):
				items.append(value)

	urls: Set[str] = set()
	for item in items:
		slug = clean_text(str(item.get("slug", "")))
		if slug:
			urls.add(normalize_property_url(config.base_url, f"/properties/{slug}/"))
			continue

		url_value = clean_text(str(item.get("url", "")))
		if url_value:
			normalized = normalize_property_url(config.base_url, url_value)
			if is_property_page_url(normalized):
				urls.add(normalized)

	return urls


def discover_property_urls(session: requests.Session, config: CrawlerConfig) -> List[str]:
	property_urls: Set[str] = set()

	listing_url = urljoin(config.base_url, config.properties_path)
	root_html = fetch_url(session, config, listing_url)
	if root_html:
		root_soup = BeautifulSoup(root_html, "html.parser")
		property_urls.update(extract_property_links_from_listing(root_soup, config.base_url))

		pagination_urls = extract_pagination_urls(root_soup, config.base_url)
		for page_url in sorted(pagination_urls):
			page_html = fetch_url(session, config, page_url)
			if not page_html:
				continue
			page_soup = BeautifulSoup(page_html, "html.parser")
			property_urls.update(
				extract_property_links_from_listing(page_soup, config.base_url)
			)

	# Fallback sequential page probing if explicit pagination links are absent.
	if root_html:
		no_new_page_streak = 0
		for page in range(2, config.max_pages + 1):
			page_url = urljoin(config.base_url, f"/properties/?page={page}")
			page_html = fetch_url(session, config, page_url)
			if not page_html:
				no_new_page_streak += 1
				if no_new_page_streak >= 1:
					break
				continue

			page_soup = BeautifulSoup(page_html, "html.parser")
			links = extract_property_links_from_listing(page_soup, config.base_url)
			if not links:
				break

			before = len(property_urls)
			property_urls.update(links)
			if len(property_urls) == before:
				no_new_page_streak += 1
			else:
				no_new_page_streak = 0

			if no_new_page_streak >= 2:
				break

	# Additional listing source from site API.
	api_urls = fetch_property_urls_from_api(session, config)
	if api_urls:
		logging.info("Discovered %d property URLs from /api/properties", len(api_urls))
	property_urls.update(api_urls)

	ordered = sorted(property_urls)
	if config.max_properties is not None:
		ordered = ordered[: config.max_properties]
	return ordered


def extract_property_name_from_page(soup: BeautifulSoup, property_url: str) -> str:
	header = soup.select_one(".banner-inner-content h1") or soup.select_one("h1")
	if isinstance(header, Tag):
		direct_text = clean_text(" ".join(header.find_all(string=True, recursive=False)))
		if direct_text:
			return direct_text
		full_text = clean_text(header.get_text(" ", strip=True))
		if full_text:
			return full_text

	slug = [part for part in urlparse(property_url).path.split("/") if part]
	if slug:
		return slug[-1].replace("-", " ").title()
	return ""


def extract_unit_links_from_property_page(soup: BeautifulSoup, base_url: str) -> Set[str]:
	unit_urls: Set[str] = set()

	# Preferred selector from requirement.
	target_lists = soup.select("ul.available-unit-list.flex.available-unit-no-image")
	for ul in target_lists:
		for anchor in ul.select("a[href]"):
			href = anchor.get("href", "")
			if not href:
				continue
			absolute = normalize_property_url(base_url, href)
			if is_unit_page_url(absolute):
				unit_urls.add(absolute)

	# Fallback scan if structure changes.
	if not unit_urls:
		for anchor in soup.select("a[href]"):
			href = anchor.get("href", "")
			if not href or "/units/" not in href:
				continue
			absolute = normalize_property_url(base_url, href)
			if is_unit_page_url(absolute):
				unit_urls.add(absolute)

	return unit_urls


def extract_detail_values(detail_block: Tag) -> Dict[str, str]:
	values: Dict[str, str] = {}
	for item in detail_block.select("ul.unit-detail-list li"):
		strong = item.find("strong")
		span = item.select_one("span.fr") or item.find("span")
		if not isinstance(strong, Tag) or not isinstance(span, Tag):
			continue

		label = clean_text(strong.get_text(" ", strip=True)).lower().rstrip(":")
		value = clean_text(span.get_text(" ", strip=True))
		if not label:
			continue

		if label.startswith("lease type"):
			key = "lease type"
		elif label.startswith("parking"):
			key = "parking"
		elif label.startswith("association dues"):
			key = "association dues"
		elif label.startswith("pet"):
			key = "pet"
		else:
			key = label

		values[key] = value

	return values


def parse_unit_modal(
	modal_container: Optional[Tag],
	property_name_fallback: str,
	unit_url: str,
	trigger_button: Optional[Tag] = None,
) -> Optional[Dict[str, str]]:
	if not isinstance(modal_container, Tag):
		return None

	detail_block = modal_container.select_one(".unit-view-right.unit-reciept")
	if not isinstance(detail_block, Tag):
		# Handle cases where the container itself is already the detail block.
		classes = set(modal_container.get("class", []))
		if {"unit-view-right", "unit-reciept"}.issubset(classes):
			detail_block = modal_container
		else:
			return None

	h2 = detail_block.find("h2")
	property_name = ""
	location = ""
	if isinstance(h2, Tag):
		property_name = clean_text(" ".join(h2.find_all(string=True, recursive=False)))
		span = h2.find("span")
		if isinstance(span, Tag):
			location = clean_text(span.get_text(" ", strip=True))

	if not property_name:
		property_name = property_name_fallback

	unit_number = ""
	h3 = detail_block.find("h3")
	if isinstance(h3, Tag):
		unit_number = clean_text(h3.get_text(" ", strip=True))

	unit_type = ""
	ud_type = detail_block.select_one("p.ud-type")
	if isinstance(ud_type, Tag):
		unit_type = clean_text(ud_type.get_text(" ", strip=True))

	price = ""
	price_tag = detail_block.select_one("p.price")
	if isinstance(price_tag, Tag):
		price = normalize_price(price_tag.get_text(" ", strip=True))

	dynamic_button = detail_block.select_one(
		"button.book-an-appointment-dynamic[data-dressing]"
	)

	raw_finish = ""
	raw_bedrooms = ""
	if isinstance(trigger_button, Tag):
		raw_finish = clean_text(trigger_button.get("data-dressing", ""))
		raw_bedrooms = clean_text(trigger_button.get("data-unit", ""))

	if isinstance(dynamic_button, Tag):
		if not raw_finish:
			raw_finish = clean_text(dynamic_button.get("data-dressing", ""))
		if not raw_bedrooms:
			raw_bedrooms = clean_text(dynamic_button.get("data-unit", ""))
		if not unit_number:
			unit_number = clean_text(dynamic_button.get("data-identifier", ""))

	unit_finish = infer_unit_finish(raw_finish or unit_type, unit_url)
	bedrooms = infer_bedrooms(raw_bedrooms or unit_type, unit_url)

	details = extract_detail_values(detail_block)
	lease_type = normalize_lease_type(details.get("lease type", ""))
	parking = normalize_parking(details.get("parking", ""))
	association_dues = normalize_association_dues(details.get("association dues", ""))
	pet_policy = normalize_pet_policy(details.get("pet", ""))

	return {
		"Property Name": property_name,
		"Unit Number": unit_number,
		"Price Range": price,
		"Location": location,
		"Unit Finish": unit_finish,
		"Bedrooms": bedrooms,
		"Lease Type": lease_type,
		"Parking": parking,
		"Association Dues": association_dues,
		"Pet Policy": pet_policy,
	}


def parse_unit_page_fallback(
	soup: BeautifulSoup,
	property_name_fallback: str,
	unit_url: str,
) -> Optional[Dict[str, str]]:
	header = soup.select_one("h1#ud-head-title")
	property_name = property_name_fallback
	unit_meta = ""

	if isinstance(header, Tag):
		property_name = clean_text(header.get("data-property-name", "")) or property_name
		span = header.find("span")
		if isinstance(span, Tag):
			unit_meta = clean_text(span.get_text(" ", strip=True))

	price = ""
	price_tag = soup.select_one(".unit-detail-head p.price")
	if isinstance(price_tag, Tag):
		price = normalize_price(price_tag.get_text(" ", strip=True))

	if not property_name and not price and not unit_meta:
		return None

	return {
		"Property Name": property_name,
		"Unit Number": "",
		"Price Range": price,
		"Location": "",
		"Unit Finish": infer_unit_finish(unit_meta, unit_url),
		"Bedrooms": infer_bedrooms(unit_meta, unit_url),
		"Lease Type": "",
		"Parking": "",
		"Association Dues": "",
		"Pet Policy": "",
	}


def extract_unit_details_with_selenium(
	config: CrawlerConfig,
	unit_url: str,
	property_name: str,
) -> List[Dict[str, str]]:
	try:
		from selenium import webdriver
		from selenium.webdriver.chrome.options import Options
		from selenium.webdriver.common.by import By
		from selenium.webdriver.common.keys import Keys
		from selenium.webdriver.support.ui import WebDriverWait
	except Exception as exc:
		logging.warning("Selenium dependencies unavailable: %s", exc)
		return []

	driver = None
	records: List[Dict[str, str]] = []
	try:
		options = Options()
		options.add_argument("--headless=new")
		options.add_argument("--disable-gpu")
		options.add_argument("--no-sandbox")
		options.add_argument("--disable-dev-shm-usage")

		driver = webdriver.Chrome(options=options)
		driver.set_page_load_timeout(config.timeout)
		driver.get(unit_url)

		WebDriverWait(driver, min(20, config.timeout)).until(
			lambda d: d.execute_script("return document.readyState") == "complete"
		)

		# If modals are JS-dependent, clicking can force hydration.
		buttons = driver.find_elements(By.CSS_SELECTOR, "a.btn.btn-ghost.btn-small.track-btn")
		for button in buttons:
			label = clean_text(button.text).lower()
			if "view unit" not in label:
				continue
			try:
				driver.execute_script("arguments[0].click();", button)
				time.sleep(0.4)
				body = driver.find_element(By.TAG_NAME, "body")
				body.send_keys(Keys.ESCAPE)
				time.sleep(0.2)
			except Exception:
				continue

		soup = BeautifulSoup(driver.page_source, "html.parser")
		for modal_container in soup.select("div[id^='unit-viewed-']"):
			record = parse_unit_modal(modal_container, property_name, unit_url)
			if record:
				records.append(record)
	except Exception as exc:
		logging.warning("Selenium fallback failed on %s: %s", unit_url, exc)
	finally:
		if driver is not None:
			try:
				driver.quit()
			except Exception:
				pass

	return dedupe_records(records)


def extract_unit_details(
	session: requests.Session,
	config: CrawlerConfig,
	property_name: str,
	unit_url: str,
) -> List[Dict[str, str]]:
	html = fetch_url(session, config, unit_url)
	if not html:
		return []

	soup = BeautifulSoup(html, "html.parser")
	records: List[Dict[str, str]] = []

	view_buttons: List[Tag] = []
	for anchor in soup.select("a.btn.btn-ghost.btn-small.track-btn, a[data-fancybox-unit]"):
		label = clean_text(anchor.get_text(" ", strip=True)).lower()
		if "view unit" in label or anchor.has_attr("data-fancybox-unit"):
			view_buttons.append(anchor)

	seen_modal_ids: Set[str] = set()
	for button in view_buttons:
		href = clean_text(button.get("href", ""))
		if not href.startswith("#unit-viewed-"):
			continue

		modal_id = href.lstrip("#")
		if not modal_id or modal_id in seen_modal_ids:
			continue
		seen_modal_ids.add(modal_id)

		modal_container = soup.find(id=modal_id)
		record = parse_unit_modal(modal_container, property_name, unit_url, button)
		if record:
			records.append(record)

	# Fallback: parse all modal containers if anchor mapping fails.
	if not records:
		for modal_container in soup.select("div[id^='unit-viewed-']"):
			record = parse_unit_modal(modal_container, property_name, unit_url)
			if record:
				records.append(record)

	# Optional JS fallback.
	if not records and config.use_selenium:
		records = extract_unit_details_with_selenium(config, unit_url, property_name)

	# Final fallback from the unit page header if no modal data is available.
	if not records:
		fallback_record = parse_unit_page_fallback(soup, property_name, unit_url)
		if fallback_record:
			records.append(fallback_record)

	return dedupe_records(records)


def parse_unit_list(
	session: requests.Session,
	config: CrawlerConfig,
	property_name: str,
	property_url: str,
	unit_urls: Sequence[str],
) -> List[Dict[str, str]]:
	all_records: List[Dict[str, str]] = []
	total_units = len(unit_urls)

	for idx, unit_url in enumerate(unit_urls, start=1):
		logging.info("    Unit page %s/%s: %s", idx, total_units, unit_url)
		try:
			unit_records = extract_unit_details(session, config, property_name, unit_url)
			if not unit_records:
				logging.warning("    No extractable unit details found at %s", unit_url)
			all_records.extend(unit_records)
		except Exception as exc:
			logging.exception("    Failed extracting unit details from %s: %s", unit_url, exc)

	if not all_records:
		logging.warning("No unit records extracted for property page %s", property_url)

	return all_records


def parse_property_page(
	session: requests.Session,
	config: CrawlerConfig,
	property_url: str,
) -> List[Dict[str, str]]:
	html = fetch_url(session, config, property_url)
	if not html:
		return []

	soup = BeautifulSoup(html, "html.parser")
	property_name = extract_property_name_from_page(soup, property_url)
	unit_links = sorted(extract_unit_links_from_property_page(soup, config.base_url))

	logging.info("  Property: %s | Unit pages found: %d", property_name, len(unit_links))

	if not unit_links:
		return []

	return parse_unit_list(session, config, property_name, property_url, unit_links)


def record_key(record: Dict[str, str]) -> Tuple[str, ...]:
	return tuple(clean_text(record.get(column, "")).lower() for column in REQUIRED_HEADERS)


def dedupe_records(records: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
	unique: List[Dict[str, str]] = []
	seen: Set[Tuple[str, ...]] = set()

	for record in records:
		key = record_key(record)
		if key in seen:
			continue
		seen.add(key)
		unique.append(record)

	return unique


def crawl_properties(session: requests.Session, config: CrawlerConfig) -> List[Dict[str, str]]:
	property_urls = discover_property_urls(session, config)
	logging.info("Discovered %d property URLs", len(property_urls))

	all_records: List[Dict[str, str]] = []
	total_properties = len(property_urls)

	for idx, property_url in enumerate(property_urls, start=1):
		logging.info("[%d/%d] Crawling property page: %s", idx, total_properties, property_url)
		try:
			property_records = parse_property_page(session, config, property_url)
			all_records.extend(property_records)
		except Exception as exc:
			logging.exception("Failed property crawl for %s: %s", property_url, exc)

	return dedupe_records(all_records)


def write_csv(records: Sequence[Dict[str, str]], output_csv: str) -> None:
	with open(output_csv, "w", encoding="utf-8", newline="") as csv_file:
		writer = csv.DictWriter(csv_file, fieldnames=REQUIRED_HEADERS)
		writer.writeheader()
		for record in records:
			writer.writerow({column: record.get(column, "") for column in REQUIRED_HEADERS})


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Crawl DMCI leasing properties and export unit data to CSV."
	)
	parser.add_argument(
		"--output",
		default="dmci_leasing_units.csv",
		help="Output CSV filename (default: dmci_leasing_units.csv)",
	)
	parser.add_argument(
		"--timeout",
		type=int,
		default=25,
		help="HTTP timeout in seconds (default: 25)",
	)
	parser.add_argument(
		"--max-retries",
		type=int,
		default=4,
		help="Maximum retries for failed HTTP requests (default: 4)",
	)
	parser.add_argument(
		"--rate-limit",
		type=float,
		default=0.35,
		help="Delay between requests in seconds (default: 0.35)",
	)
	parser.add_argument(
		"--max-pages",
		type=int,
		default=20,
		help="Max listing pages to probe for pagination fallback (default: 20)",
	)
	parser.add_argument(
		"--max-properties",
		type=int,
		default=0,
		help="Limit number of properties to crawl for testing (0 = no limit)",
	)
	parser.add_argument(
		"--use-selenium",
		action="store_true",
		help="Use Selenium fallback if View Unit data is JS-dependent",
	)
	parser.add_argument(
		"--verbose",
		action="store_true",
		help="Enable verbose logging",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	logging.basicConfig(
		level=logging.DEBUG if args.verbose else logging.INFO,
		format="%(asctime)s | %(levelname)s | %(message)s",
	)

	max_properties = args.max_properties if args.max_properties and args.max_properties > 0 else None

	config = CrawlerConfig(
		output_csv=args.output,
		timeout=args.timeout,
		max_retries=args.max_retries,
		rate_limit_seconds=max(0.0, args.rate_limit),
		max_pages=max(1, args.max_pages),
		max_properties=max_properties,
		use_selenium=args.use_selenium,
	)

	session = build_session()

	logging.info("Starting crawl at %s", urljoin(config.base_url, config.properties_path))
	records = crawl_properties(session, config)
	write_csv(records, config.output_csv)
	logging.info("Done. Wrote %d unique rows to %s", len(records), config.output_csv)


if __name__ == "__main__":
	main()
