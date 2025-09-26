import asyncio
import azure.functions as func
import azure.durable_functions as df

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@myApp.route(route="orchestrators/hello_orchestrator")
@myApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    max_retries = 3
    base_delay = 2  # seconds
    for attempt in range(max_retries):
        try:
            instance_id = await client.start_new('hello_orchestrator')
            # response = client.create_http_response_function.create_check_status_response(req, instance_id)
            response = client.create_check_status_response(req, instance_id)
            return response
        except Exception as ex:
            if "An error occurred while communicating with Azure Storage" in str(ex) and attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                print(f"Creation attempt {attempt+1} failed. Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                # Re-raise the exception if it's the last attempt or a different error
                raise ex


# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def hello_orchestrator(context):
    result1 = yield context.call_activity("hello", "Seattle")
    result2 = yield context.call_activity("hello", "Tokyo")
    result3 = yield context.call_activity("hello", "London")

    return [result1, result2, result3]

# Activity
@myApp.activity_trigger(input_name="city")
def hello(city: str):
    return f"Hello {city}"