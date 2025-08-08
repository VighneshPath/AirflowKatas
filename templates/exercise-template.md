# Exercise X: [Exercise Title]

## Objective

[Clear, specific statement of what students will accomplish in this exercise. Should be measurable and achievable.]

**Learning Goals:**

- [ ] Goal 1: Specific skill or knowledge to be gained
- [ ] Goal 2: Another measurable learning outcome
- [ ] Goal 3: Additional objective for this exercise

## Prerequisites

**Required Knowledge:**

- [ ] Concept 1: From previous modules or exercises
- [ ] Concept 2: Another prerequisite concept
- [ ] Concept 3: Additional required knowledge

**Required Completion:**

- [ ] Module XX: Previous module that must be completed
- [ ] Exercise X-X: Specific previous exercise if applicable

## Scenario

[Provide a realistic, engaging scenario that gives context for the exercise. This should be a real-world situation that students might encounter.]

**Background:**
[Additional context about the scenario, including any business requirements or constraints.]

**Your Role:**
[Describe the student's role in this scenario - are they a data engineer, analyst, etc.?]

## Requirements

### Functional Requirements

1. **Requirement 1**: [Specific, testable requirement]

   - Acceptance criteria: [How to verify this requirement is met]
   - Expected outcome: [What should happen when this is implemented]

2. **Requirement 2**: [Another specific requirement]

   - Acceptance criteria: [Verification method]
   - Expected outcome: [Expected result]

3. **Requirement 3**: [Additional requirement]
   - Acceptance criteria: [How to test]
   - Expected outcome: [What should occur]

### Technical Requirements

- **DAG Configuration**: [Specific DAG settings required]
- **Task Requirements**: [Types and number of tasks needed]
- **Dependencies**: [How tasks should be connected]
- **Error Handling**: [Required error handling approach]
- **Scheduling**: [Scheduling requirements if applicable]

### Quality Requirements

- **Code Quality**: Follow Python and Airflow best practices
- **Documentation**: Include clear comments and docstrings
- **Testing**: Ensure DAG can be parsed and validated
- **Performance**: Consider efficiency and resource usage

## Instructions

### Step 1: Setup and Planning

[Provide initial setup instructions and planning guidance]

1. Create a new DAG file named `[suggested_filename].py`
2. Set up the basic DAG structure with appropriate configuration
3. Plan your task structure and dependencies

**Hints:**

- Consider what imports you'll need
- Think about the logical flow of your tasks
- Plan for error handling from the start

### Step 2: [Major Step Name]

[Detailed instructions for the first major implementation step]

1. Substep 1: [Specific action to take]
2. Substep 2: [Another specific action]
3. Substep 3: [Additional action]

**Code Template:**

```python
# Provide a helpful code template or starting point
from airflow import DAG
from datetime import datetime

# Template code here
```

**Hints:**

- Hint 1: [Helpful guidance without giving away the solution]
- Hint 2: [Another useful tip]

### Step 3: [Second Major Step]

[Instructions for the next major step]

1. Implementation detail 1
2. Implementation detail 2
3. Implementation detail 3

**Important Considerations:**

- Consideration 1: [Important factor to keep in mind]
- Consideration 2: [Another key consideration]

### Step 4: [Final Step]

[Instructions for completing the exercise]

1. Final implementation steps
2. Testing and validation
3. Documentation and cleanup

## Validation

### Self-Check Questions

Before submitting your solution, verify:

- [ ] **Question 1**: Does your DAG parse without errors?
- [ ] **Question 2**: Are all requirements met?
- [ ] **Question 3**: Does the task flow make logical sense?
- [ ] **Question 4**: Is your code well-documented?

### Testing Your Solution

1. **Syntax Validation**: Run `python scripts/validate_dags.py --dag-file your_dag.py`
2. **DAG Parsing**: Ensure your DAG appears in the Airflow UI without errors
3. **Task Execution**: Test individual tasks if possible
4. **End-to-End**: Verify the complete workflow functions as expected

### Expected Output

[Describe what students should see when their solution is working correctly]

**Success Indicators:**

- Indicator 1: [What should be visible/working]
- Indicator 2: [Another success sign]
- Indicator 3: [Additional confirmation]

## Extension Challenges

### Challenge 1: [Optional Enhancement]

[Description of an optional challenge that extends the basic exercise]

**Additional Requirements:**

- Requirement 1: [Extra functionality to implement]
- Requirement 2: [Another enhancement]

**Hints:**

- Consider using [relevant Airflow feature]
- Look into [additional concept or tool]

### Challenge 2: [Another Optional Challenge]

[Description of a second optional challenge]

### Challenge 3: [Advanced Challenge]

[Description of a more advanced optional challenge for experienced students]

## Common Pitfalls

### Pitfall 1: [Common Mistake]

**Problem**: [Description of the common mistake]
**Symptoms**: [How students might recognize this issue]
**Solution**: [How to avoid or fix this mistake]

### Pitfall 2: [Another Common Issue]

**Problem**: [Another frequent mistake]
**Symptoms**: [Signs of this problem]
**Solution**: [Prevention or resolution approach]

### Pitfall 3: [Additional Common Error]

**Problem**: [Third common mistake]
**Symptoms**: [How to identify this issue]
**Solution**: [How to address it]

## Troubleshooting

### Issue 1: [Specific Problem]

**Symptoms:**

- Symptom 1
- Symptom 2

**Possible Causes:**

- Cause 1: [Likely cause and solution]
- Cause 2: [Another possible cause and fix]

### Issue 2: [Another Problem]

**Symptoms:**

- Different symptom 1
- Different symptom 2

**Solutions:**

1. Solution step 1
2. Solution step 2
3. Verification step

## Resources

### Relevant Documentation

- [Link 1](https://example.com): Description of relevant docs
- [Link 2](https://example.com): Another helpful resource
- [Link 3](https://example.com): Additional documentation

### Code Examples

- [Example 1](https://example.com): Similar implementation example
- [Example 2](https://example.com): Related code pattern
- [Example 3](https://example.com): Additional reference

### Tutorials

- [Tutorial 1](https://example.com): Step-by-step guide for related concepts
- [Tutorial 2](https://example.com): Another helpful tutorial

## Reflection Questions

After completing the exercise, consider:

1. **Understanding**: What was the most challenging part of this exercise?
2. **Application**: How might you apply these concepts in a real project?
3. **Improvement**: What would you do differently if you started over?
4. **Extension**: What additional features could enhance this solution?
5. **Learning**: What new concepts did you learn or reinforce?

## Next Steps

### Immediate Next Steps

- [ ] Compare your solution with the provided solution
- [ ] Try the extension challenges if you haven't already
- [ ] Review any concepts that were challenging

### Preparation for Next Exercise

- [ ] Review [specific concept] that will be important next
- [ ] Ensure you understand [key learning] from this exercise
- [ ] Practice [skill] that builds on this exercise

### Further Learning

- [ ] Explore the additional resources provided
- [ ] Try applying these concepts to a personal project
- [ ] Share your solution and get feedback from others

---

## Submission Guidelines

### File Naming

- Main DAG file: `exercise_X_[descriptive_name].py`
- Additional files: Follow the naming convention in the instructions

### Code Requirements

- Include comprehensive comments
- Follow PEP 8 style guidelines
- Use meaningful variable and function names
- Include docstrings for functions

### Documentation

- Brief explanation of your approach
- Any assumptions you made
- Challenges you encountered and how you solved them
- Ideas for future improvements

---

**Ready to start?** Begin with Step 1 and work through each section systematically. Remember, the goal is learning - don't hesitate to experiment and ask questions!

**Need help?** Check the troubleshooting section, review the relevant concepts, or ask for guidance in the community discussions.
