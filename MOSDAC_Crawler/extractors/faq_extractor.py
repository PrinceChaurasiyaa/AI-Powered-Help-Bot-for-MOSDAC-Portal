from typing import List, Tuple
import requests
from bs4 import BeautifulSoup, Tag
from utils.logger import get_logger

log = get_logger(__name__)

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
URL = "https://www.mosdac.gov.in/faq-page"

header = {
    "User-Agent": user_agent,
    "Referer": "https://www.mosdac.gov.in"
}
response = requests.get(URL, headers=header, timeout=15)
soup = BeautifulSoup(response.text, "lxml")

class FAQExtractor:
    def extract(
            self, 
            soup: BeautifulSoup, 
            url: str 
        ) -> List[Tuple[str, str, str]]:
        
        results: List[Tuple[str, str, str]] = []

        results.extend(self._extract_manual(soup))

        # Deduplicate by Question text
        seen: set = set()
        unique = []
        for que, ans, method in results:
            key = que.lower().strip()
            if key and key not in seen and len(ans) > 10:
                seen.add(key)
                unique.append((que.strip(), ans.strip(), method))
        log.debug(f"FAQExtractor: {len(unique)} Q&A pairs found at {url}")
        return unique

    def _extract_manual(
            self, 
            soup: BeautifulSoup
        ) -> List[Tuple[str, str, str]]:

        faqs = []

        for item in soup.select("div.faq-question-answer"):
            q_tag = item.select_one("div.faq-question span[property='dc:title']")
            question = q_tag.get_text(strip=True) if q_tag else ""

            a_tag = item.select_one("div.faq-answer .field-item")
            if a_tag:
                for tag in a_tag.find_all(["p", "span"]):
                    text = tag.get_text()
                    if "<w:" in text or "LsdException" in text:
                        tag.decompose()
                
                answer = a_tag.get_text(separator=" ", strip=True)

                answer = " ".join(answer.split())
            else:
                answer = ""
            
            if question and answer:
                faqs.append((question, answer, "Manual"))
                
                print(f"Q: {question}\nA: {answer[:100]}...\n")

        print(f"\nTotal FAQs extracted : {len(faqs)}")
        log.info(faqs)
        log.info(f"\nTotal FAQs extracted : {len(faqs)}")

        return faqs

