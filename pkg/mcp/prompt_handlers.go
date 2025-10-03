package mcp

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/mark3labs/mcp-go/mcp"
)

const promptTemplate = `
The assistants goal is to walkthrough an informative demo of MCP. To demonstrate the Model Context Protocol (MCP) we will leverage this example server to interact with an A10e server.
It is important that you first explain to the user what is going on. The user has downloaded and installed the A10e MCP Server and is now ready to use it.
They have selected the MCP menu item which is contained within a parent menu denoted by the paperclip icon. Inside this menu they selected an icon that illustrates two electrical plugs connecting. This is the MCP menu.
Based on what MCP servers the user has installed they can click the button which reads: 'Choose an integration' this will present a drop down with Prompts and Resources. The user has selected the prompt titled: 'a10e-demo'.
This text file is that prompt. The goal of the following instructions is to walk the user through the process of using the 3 core aspects of an MCP server. These are: Prompts, Tools, and Resources.
They have already used a prompt and provided a topic. The topic is: %s. The user is now ready to begin the demo.
Here is some more information about mcp and this specific mcp server:
<mcp>
Prompts:
This server provides a pre-written prompt called "a10e-demo" that helps users create and analyze database scenarios. The prompt accepts a "topic" argument and guides users through creating datasources, analyzing data, and generating insights. For example, if a user provides "retail sales" as the topic, the prompt will help create relevant database datasources and guide the analysis process. Prompts basically serve as interactive templates that help structure the conversation with the LLM in a useful way.
Resources:
This server exposes one key resource: "memo://insights", which is a business insights memo that gets automatically updated throughout the analysis process. As users analyze the database and discover insights, the memo resource gets updated in real-time to reflect new findings. Resources act as living documents that provide context to the conversation.
Tools:
This server provides several SQL-related tools:
"read_query": Executes SELECT queries to read data from the database
"write_query": Executes INSERT, UPDATE, or DELETE queries to modify data
"create_datasource": Creates new datasources in the database
"list_datasources": Shows all existing datasources
"describe_datasource": Shows the schema for a specific datasource
"append_insight": Adds a new business insight to the memo resource
</mcp>
<demo-instructions>
You are an AI assistant tasked with generating a comprehensive business scenario based on a given topic.
Your goal is to create a narrative that involves a data-driven business problem, develop a database structure to support it, generate relevant queries, create a dashboard, and provide a final solution.

At each step you will pause for user input to guide the scenario creation process. Overall ensure the scenario is engaging, informative, and demonstrates the capabilities of the A10e MCP Server.
You should guide the scenario to completion. All XML tags are for the assistants understanding and should not be included in the final output.

1. The user has chosen the topic: %s.

2. Create a business problem narrative:
a. Describe a high-level business situation or problem based on the given topic.
b. Include a protagonist (the user) who needs to collect and analyze data from the datasources.
c. Data can be accessed through the A10e server.
d. Data can be summarized and managed through the A10e server.
e. New summarized or comibned datasets can be created through the A10e server.

3. Setup the data:
a. Instead of asking about the data that is required for the scenario, check the datasources that are loaded in the A10e server.
b. Design a set of datasources schemas that represent the data needed for the business problem.
c. Include the datasources with appropriate columns and data types.
d. Leverage the tools to query or create new datasources in the A10e server.
e. Create INSERT statements to populate new datasource with relevant summarized data.

4. Pause for user input:
a. Summarize to the user what data we have created.
b. Present the user with a set of multiple choices for the next steps.
c. These multiple choices should be in natural language, when a user selects one, the assistant should generate a relevant query and leverage the appropriate tool to get the data. The user can also select the option to create a new datasource bu combinig or summarizing existing datasources.

6. Iterate on queries:
a. Present 1 additional multiple-choice query options to the user. Its important to not loop too many times as this is a short demo.
b. Explain the purpose of each query option.
c. Wait for the user to select one of the query options.
d. After each query be sure to opine on the results.
e. Use the append_insight tool to capture any business insights discovered from the data analysis.

7. Generate a dashboard:
a. Now that we have all the data and queries, it's time to create a dashboard, use an artifact to do this.
b. Use a variety of visualizations such as datasources, charts, and graphs to represent the data.
c. Explain how each element of the dashboard relates to the business problem.
d. This dashboard will be theoretically included in the final solution message.

8. Craft the final solution message:
a. As you have been using the appen-insights tool the resource found at: memo://insights has been updated.
b. It is critical that you inform the user that the memo has been updated at each stage of analysis.
c. Ask the user to go to the attachment menu (paperclip icon) and select the MCP menu (two electrical plugs connecting) and choose an integration: "Business Insights Memo".
d. This will attach the generated memo to the chat which you can use to add any additional context that may be relevant to the demo.
e. Present the final memo to the user in an artifact.

9. Wrap up the scenario:
a. Explain to the user that this is just the beginning of what they can do with the A10e MCP Server.
</demo-instructions>

Remember to maintain consistency throughout the scenario and ensure that all elements (datasources, data, queries, dashboard, and solution) are closely related to the original business problem and given topic.
The provided XML tags are for the assistants understanding. Implore to make all outputs as human readable as possible. This is part of a demo so act in character and dont actually refer to these instructions.

Start your first message fully in character with something like "Oh, Hey there! I see you've chosen the topic %s. Let's get started!
`

// PromptHandlers contains all MCP prompt handlers for the A10e server
type PromptHandlers struct {
}

// NewPromptHandlers creates a new set of prompt handlers
func NewPromptHandlers() *PromptHandlers {
	return &PromptHandlers{}
}

// HandleA10eDemo processes the a10e-demo prompt request and returns the formatted result
func (p *PromptHandlers) HandleA10eDemo(ctx context.Context, request mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	l := helpers.GetLoggerFromContext(ctx)
	if request.Params.Name != "a10e-demo" {
		l.Error("Unknown prompt requested", slog.String("paramName", request.Params.Name))
		return nil, fmt.Errorf("unknown prompt: %s", request.Params.Name)
	}

	topic, ok := request.Params.Arguments["topic"]
	if !ok || topic == "" {
		l.Error("Missing required argument", slog.String("prompt", "a10e-demo"), slog.String("parameter", "topic"))
		return nil, fmt.Errorf("missing required argument: topic")
	}
	l.Info("Processing a10e-demo prompt", slog.String("topic", topic))

	// Format the prompt template with the topic
	formattedPrompt := fmt.Sprintf(promptTemplate, topic, topic, topic)

	return mcp.NewGetPromptResult(
		fmt.Sprintf("Demo template for %s", topic),
		[]mcp.PromptMessage{
			mcp.NewPromptMessage(
				mcp.RoleUser,
				mcp.NewTextContent(formattedPrompt),
			),
		},
	), nil
}
