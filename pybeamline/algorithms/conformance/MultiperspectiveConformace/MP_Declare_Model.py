import xml.etree.ElementTree as ET
import re
from .Templates.Response import Response
from .Templates.TemplateProtocol import Template

class Constraint_Condition():
    def __init__(self, A:set, T:set, phi_a, phi_c, phi_tau) -> None:
        self.A = A
        self.T = T
        self.phi_a = phi_a
        self.phi_c = phi_c
        self.phi_tau = phi_tau

    def parse_string_to_constraint_condition(input_string: str):
    # Regular expression patterns to extract A and T
        A_pattern = r"\[A\.concept:name == '(.*?)'\]"
        T_pattern = r"\[T\.concept:name == '(.*?)'\]"
        phi_pattern = r"\[(\d+),(\d+),(\w+)\]"
        
        # Extract A and T
        A_match = re.search(A_pattern, input_string)
        T_match = re.search(T_pattern, input_string)
        
        # Extract phi values
        phi_match = re.search(phi_pattern, input_string)
        
        if A_match and T_match and phi_match:
            # Horrible implementation, if there are more than one, but that doesnt apply to our current models in xml
            A = set()
            A.add(A_match.group(1))
            T = set()
            T.add(T_match.group(1))
            phi_a = int(phi_match.group(1))
            phi_c = int(phi_match.group(2))
            phi_tau = phi_match.group(3)
            
            # Create and return the Constraint_Condition object
            return Constraint_Condition(A, T, phi_a, phi_c, phi_tau)
        else:
            raise ValueError("Input string is not in the expected format.")
        
    def __repr__(self) -> str:
        return (f"A: {self.A}, T: {self.T}, Tau: [{self.phi_a, self.phi_c, self.phi_tau}]")

class Constraint():
    def __init__(self, id:str, manadatory:bool, condition:Constraint_Condition, name:str, template, constraint_parameters:list) -> None:
        self.id = id
        self.manadatory = manadatory,
        self.condition = condition,
        self.name = name,
        self.template = template,
        self.constraint_parameters = constraint_parameters

class MP_declare_constraintparameter:
    def __init__(self, name:int, branches:list) -> None:
        self.name = name
        self.branches = branches

class MP_delcare_model:
    def __init__(self, name:str, language:str, activities, constraints) -> None:
        self.name = name
        self.language = language
        self.activities = activities # type: ignore
        self.constraints = constraints # type: ignore

    @classmethod
    def from_xml(cls, xml_file):
        tree = ET.parse(xml_file)
        model = tree.getroot()

        name = model.find("assignment").get('name')
        language = model.find("assignment").get('language')

        # Parse activities
        activities = {}
        for activity in model.find("assignment").find("activitydefinitions"):
            activity_id = activity.get("id")
            activity_name = activity.get("name")
            activities[activity_id] = activity_name # type: ignore

        # Parse Constraints
        constraints = []
        for constraint in model.find("assignment").find("constraintdefinitions"):
            constraint_id = constraint.get("id")
            constraint_mandatory = constraint.get("mandatory") == 'true'
            constraint_name = constraint.find("name").text

            # Map to template class
            if (constraint_name == "response"):
                constraint_template = Response()
            else:
                constraint_template = Template()
            
            # Map to condition class
            constraint_condition = Constraint_Condition.parse_string_to_constraint_condition(constraint.find("condition").text)

            constraint_parameters = []
            for parameter in constraint.find("constraintparameters"):
                parameter_name = parameter.get("templateparameter")
                parameter_branches = []
                for branch in parameter.find("branches").find("branch"):
                    parameter_branches.append(branch.get("name"))
                constraint_parameters.append(MP_declare_constraintparameter(parameter_name, parameter_branches))

            constraints.append(Constraint(
                constraint_id,
                constraint_mandatory,
                constraint_condition,
                constraint_name,
                constraint_template,
                constraint_parameters
            ))


        return cls(name, language, activities, constraints)
    
    def __repr__(self):
        return (f"Model(name={self.name}, activities={self.activities}, "
                f"constraints={len(self.constraints)} constraints)")
    
    def to_xml(self):
        model = ET.Element("model")
        assignment = ET.SubElement(model, "assignment", language=self.language, name=self.name)

        # Add activity definitions
        activity_definitions = ET.SubElement(assignment, "activitydefinitions")
        for activity_id, activity_name in self.activities.items():
            ET.SubElement(activity_definitions, "activity", id=activity_id, name=activity_name)

        # Add constraint definitions
        constraint_definitions = ET.SubElement(assignment, "constraintdefinitions")
        for constraint in self.constraints:
            constraint_element = ET.SubElement(
                constraint_definitions, 
                "constraint", 
                id=constraint['id'], 
                mandatory=str(constraint['mandatory']).lower()
            )
            ET.SubElement(constraint_element, "condition").text = constraint['condition']
            ET.SubElement(constraint_element, "name").text = constraint['name']
            
            template_element = ET.SubElement(constraint_element, "template")
            template_element.text = constraint['template']

            constraint_parameters = ET.SubElement(constraint_element, "constraintparameters")
            for param in constraint['parameters']:
                param_element = ET.SubElement(
                    constraint_parameters, 
                    "parameter", 
                    templateparameter=param.name
                )
                branches = ET.SubElement(param_element, "branches")
                for branch_name in param.branches:
                    ET.SubElement(branches, "branch", name=branch_name)

        # Generate XML string
        return ET.tostring(model, encoding="unicode", method="xml")
                
    def add_constraint(self, constraint:Constraint):
        self.constraints.append(constraint)

    def remove_constraint(self, constraint):
        self.constraints.remove(constraint) 

    def get_constraints(self):
        return self.constraints 


if __name__ == "__main__":
    model = MP_delcare_model.from_xml("pybeamline/algorithms/conformance/MultiperspectiveConformace/dummy_models/model-10-constraints-data.xml")

    for constraint in model.constraints:
        print(constraint.condition)
