# Launch draft

Title:

Show HN: A Temporal integration for Pi Durable

Post:

I built a small Temporal integration for Pi Durable. Pi already provides durable agent execution. This adapter runs Pi's loop inside a Temporal workflow and runs model calls and tools as activities.

The reason to use it is Temporal's infrastructure. Workers poll task queues. You can retry activities separately and inspect their recorded results. Human approval can wait in a workflow until someone answers.

The local demo uses a model to fix a JavaScript module. It checks the generated source without executing it, then requests publication approval. After publication, the demo kills the worker before activity completion and starts a replacement. In the recorded run, four completed model calls were reused and the file was published once. The approval in that recording was simulated through the API. You can run the example with an approval decision in the browser instead.

There is also a console example with automatic approval evaluations and observable plan state. The UI is Temporal's community Agent Harness console. The example server implements the parts of its protocol used here.

The adapter is experimental. The package pins the supported Pi and Temporal versions. All deployment testing has been local. The README explains payload storage and the remaining limits.

Repository: https://github.com/h0rv/pi-durable-temporal

## Before posting

Check the release workflow and recording. Submit the post from the project owner's account. This draft has not been posted.
