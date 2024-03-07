## OpenAI Lib
from openai import OpenAI
import json
from utils.neo4jHandler import SendDataNeo4j


class EitreeGPT:
    def __init__(self, api_key: str) -> None:
        self.client = OpenAI(api_key=api_key)

    def __get_completion__(self, prompt, model="gpt-3.5-turbo", temperature: int = 0):
        """
        This function calls ChatGPT API with a given prompt
        and returns the response back.
        """
        messages = [{"role": "user", "content": prompt}]
        response = self.client.chat.completions.create(
            model=model, messages=messages, temperature=temperature
        )
        try:
            return response.choices[0].message.content
        except:
            return None

    def define_prompt_project(self, query: str):

        prompt_project = f"""
        You're a technology project manager.
        You will classify which technologies are best to be used in a given project.
        Only if they are mentioned in the description.
        If not, you say: It sounds like your project is very specific, do you want to set up a meeting to talk about it?
        If there is any correlation between technologies, you suggest the correlations.

        project description: {query}

        Always contemplate the whole scenario.
        Don't describe the technologies, just suggest them.
        Focus on the core of the project and the best technologies for it.
        Describe the level of knowledge required for each technology in the project, on a scale of 1 to 5.
        The output should contain only the technology and level of expertise required.

        Among these skills, try to approximate the correlated ones that are mentioned in the project.
        Skills : 
            ['Backend', 'API Management / Orchestration', 'Mobile AR', 'Unit Testing', 
            'Linux', 'Voice', 'Agile Development', 'Git', 'IoT', 'Accessibility', 
            'Responsiviness', 'ORM', 'Animation', 'Blockchain', 'Dast / Sast', 
            'Serverless Architecture', 'WebSocket', 'SEO', 'Frontend', 'NoSQL', 
            'Data Science', 'Clean Code', 'Devops', 'Microservices Architecture', 
            'CI / CD', 'Automated Testing', 
            'Design Pattern', 'SQL', 'TDD', 
            'Network', 'Mixed Reality (AR/VR/XR)', 
            'REST', 'MacOs', 'Cyber Security', 'HTML', 
            'CSS', 'Python', 'Java', 'PHP', 'C++', 'Kotlin', 
            'Rust', 'Flutter', 'Angular', 'React Native', 'React', 
            'Redux', 'Next.js', 'Typescript', 'Ruby', 'SOLID', 'Sass', 
            'Gulp', 'Yarn', 'Jest', 'Machine Learning', 'ASP.NET MVC', 
            'Xamarin / Maui', 'Blazor', 'AWS', 'Shift', 'CI/CD', 'Apache', 
            'Azure', 'IIS', 'Ngnix', 'Windows', 'C#', 'Kubernets', 'Javascript', 
            '.NET', 'Docker', 'C', 'Octopus', 'Go', 'Vue', 'Google Tag Manager', 
            'jQuery', 'Entity Framework', '.NET Core', 'Pug.js', 'Salesforce Marketing Cloud', 
            'Postgres', 'Firebase', 'Selenium', 'Gunicorn', 'Cypress', 'Google Analytics', 'Flask', 
            'Rails', 'Adobe Analytics', 'Adobe Launch', 'Node.js', 'Vuforia', 'NPM', 'RPC', 'Spring', 
            'Team Management', 'English Level', 'Adobe Target', 'AEM', 'WordPress', 'Drupal', 'Shell Scripting', 
            'Sitecore', 'Project Management', 'Wix', 'Criativity', 'Problem Solving', 'Client Relations', 'Colaboration', 
            'Empathy', 'Time Management', 'Express', 'Leadership', 'Nest.js', 'Microsoft Sharepoint', 'Nintex forms', 'Redis', 
            'Nintex Workflows', 'Sharegate']

        Knowledge Level Classifier :
            0- I don't know this tech
            1- Basic Proficiency
            2- Learning the skill
            3- Comfortable working with this skill
            4- Expert at this skill
            5- Can teach others this skill

        Don't write techs in other format, get exactly representation of Skills list to classify.
        Make a json in this format : tech : level
        Order by major level to minor level
        Remember if no tech or programming language as cited. 
        Say "It sounds like your project is very specific, do you want to set up a meeting to talk about it?"
        """
        return prompt_project

    def get_techs_from_description(self, description: str) -> json:

        description_project = self.define_prompt_project(query=description)
        output = self.__get_completion__(prompt=description_project)

        if "It sounds like your project is very specific" in output:
            return output
        return json.loads(output)


class Neo4jMatchPeople(SendDataNeo4j):

    def __init__(self, uri, user, password):
        super().__init__(uri, user, password)

    def mount_query_match_people(self, output_project: dict) -> str:

        top_three_techs = list(output_project.items())[:3]

        query = "MATCH (n:PEOPLE)-[r:KNOWS]->(t:TECH) WHERE"

        for idx, (tech, level) in enumerate(top_three_techs):

            query += f"(t.name = '{tech}' AND r.skill_level >= {level})"
            if idx < len(top_three_techs) - 1:
                query += " OR "

        query += "RETURN COUNT(DISTINCT n)"

        return query

    def search_people(self, query: str) -> int:

        with self.driver.session() as session:
            print(query)
            result = session.read_transaction(self._run_query, query)
            return result[0][0]
