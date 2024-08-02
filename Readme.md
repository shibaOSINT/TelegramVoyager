To test a particular container, you'll most likely need env variable that are set in the .env file. To use it just run `docker run --env-file .env my_docker_image`

You'll need to have a session file to make Telegram requests. Generate it beforehand by running crawler.py locally (after replacing the API_ID and API_HASH values by yours.)


To start the spider, start at the root folder of this repo:

`docker compose -f docker-compose-spider.yaml --env-file .env-spider -p telegram-voyager-spider up --build --force-recreate`

To start the orchestrator, start at the root folder of this repo:

`docker compose -f docker-compose-orchestrator.yaml --env-file .env-orchestrator -p telegram-voyager-orchestrator up`


To inject a channel and start the crawler:

`diag -i infrarotsichtinsdunkel -1001742533871`