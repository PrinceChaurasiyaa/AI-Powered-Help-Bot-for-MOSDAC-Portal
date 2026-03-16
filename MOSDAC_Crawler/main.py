from crawler.mosdacSiteMap import MOSDACsitemap
from storage.data_store import DataStore
from utils.logger import get_logger
log     = get_logger(__name__)

def main():
    store = DataStore()
    seeder = MOSDACsitemap(store)
    pages, docs = seeder.seed_all()

    print("Seed complete")
    print("Pages:", pages)
    print("Documents:", docs)

if __name__ == "__main__":
    main()