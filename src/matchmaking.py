from eitreeGPT import EitreeGPT, Neo4jMatchPeople
from flask import Flask, request
from flask_cors import CORS
import json
from decouple import config

# DOTENV_FILE = "/home/nophaieviscente/eitree/eitreeDeal/.env"  # Path to .env
# config = Config(RepositoryEnv(DOTENV_FILE))
RAW_DATA_PATH = config("RAW_DATA")
NEO4J_URI = config("NEO4J_URI")
NEO4J_PASSWORD = config("NEO4J_PASSWORD")
NEO4J_USER = config("NEO4J_USER")
OPENAI_API_KEY = config("OPENAI_KEY")


bot = Flask(__name__)
CORS(app=bot)

gepeto_eitree = EitreeGPT(api_key=OPENAI_API_KEY)
neo4j_handler = Neo4jMatchPeople(
    uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD
)


@bot.route("/matchmaking", methods=["POST"])
def input_description():
    request_data = request.get_json()
    description = request_data.get("description")
    print(f"Description: {description}")
    techs_from_project = gepeto_eitree.get_techs_from_description(
        description=description
    )
    if type(techs_from_project) == str:
        return json.dumps({"matches": 0})
    print(techs_from_project)
    techs_from_project = {
        tech.lower(): level for tech, level in techs_from_project.items()
    }
    neo4j_query = neo4j_handler.mount_query_match_people(
        output_project=techs_from_project
    )
    matches = neo4j_handler.search_people(query=neo4j_query)
    print(matches)
    return json.dumps({"matches": matches})


if __name__ == "__main__":
    bot.run(debug=True)
