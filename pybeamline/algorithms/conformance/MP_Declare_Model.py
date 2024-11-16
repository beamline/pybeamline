import xml.etree.ElementTree as ET

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
            constraint_template = constraint.find("template").text
            # Map to condition class
            constraint_condition = constraint.find("condition").text

            constraint_parameters = []
            for parameter in constraint.find("constraintparameters"):
                parameter_name = parameter.get("templateparameter")
                parameter_branches = []
                for branch in parameter.find("branches").find("branch"):
                    parameter_branches.append(branch.get("name"))
                constraint_parameters.append(MP_declare_constraintparameter(parameter_name, parameter_branches))


            constraints.append({
                'id': constraint_id,
                'mandatory': constraint_mandatory,
                'condition': constraint_condition,
                'template' : constraint_template,
                'name': constraint_name,
                'parameters': constraint_parameters
            })


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
                

class MP_declare_constraintparameter:
    def __init__(self, name:int, branches:list) -> None:
        self.name = name
        self.branches = branches



if __name__ == "__main__":
    print(MP_delcare_model.from_xml("pybeamline/algorithms/conformance/model-10-constraints-data.xml"))
