# Interpolation Engine

The interpolation engine provides a mechanism to dynamically replace placeholders in a string with values from an `eventing.MdaiEvent`. It is inspired by similar concepts in OpenTelemetry and other observability tools.

## Syntax

The interpolation syntax follows a simple pattern:
`${scope:field:-defaultValue}
`

- **`scope`**: The source of the data. Currently, the only supported scope is `trigger`.
- **`field`**: The path to the value you want to retrieve from the event. This can be a top-level event attribute or a nested path within the event's JSON payload.
- **`defaultValue`**: (Optional) A fallback value to use if the specified `field` cannot be found or its value is considered "empty" (e.g., an empty string or a zero-value timestamp).

---

## Supported Scope: `trigger`

The `trigger` scope refers to the `eventing.MdaiEvent` object that is being processed. Any scope other than `trigger` is unsupported and will cause the placeholder to be returned as-is.

### Top-Level Event Fields

You can access the direct fields of the `MdaiEvent` struct.

**Example:**
`"Event ID is ${trigger:id}"` will be interpolated to `"Event ID is test-id"`.

| Field Name      | `MdaiEvent` Field | Notes                                                              |
|-----------------|-------------------|--------------------------------------------------------------------|
| `id`            | `ID`              | The unique identifier of the event.                                |
| `name`          | `Name`            | The name of the event.                                             |
| `timestamp`     | `Timestamp`       | The event timestamp, formatted as an RFC3339 string (e.g., `2023-01-01T12:00:00Z`). |
| `source`        | `Source`          | The source of the event.                                           |
| `source_id`     | `SourceID`        | The ID from the source system.                                     |
| `correlation_id`| `CorrelationID`   | The correlation ID for tracing.                                    |
| `hub_name`      | `HubName`         | The name of the hub that processed the event.                      |
| `payload`       | `Payload`         | The entire raw JSON payload as a string.                           |

### Payload Fields

To access values within the event's JSON payload, use the `payload.` prefix followed by a dot-separated path.

**Example Event Payload:**
```json
{
  "user": {
    "name": "John",
    "details": {
      "age": 30
    }
  },
  "level": "info",
  "tags": ["a", "b"]
}
```

Example Interpolations:  

| Input String                          | Resulting String          | Description                                       |
|---------------------------------------|---------------------------|---------------------------------------------------|
| `${trigger:payload.level}`            | `info`                    | Access a top-level key in the payload.            |
| `${trigger:payload.user.name}`        | `John`                    | Access a nested key.                              |
| `${trigger:payload.user.details.age}` | `30`                      | Access a deeply nested key.                       |
| `${trigger:payload.tags}`             | `["a","b"]`               | An array is returned as a JSON string.            |
| `${trigger:payload.user}`             | `{"name":"John",...}`     | An object is returned as a JSON string.           |

### Default Values
You can provide a default value using the :- syntax. The default value is used in the following cases:  
1. The specified field does not exist (e.g., ${trigger:payload.nonexistent:-default}).
2. The field exists but its value is considered empty (e.g., an empty string for Name or a zero-value for Timestamp). 

Examples:  

| Input String                               | Condition                               | Resulting String |  
|--------------------------------------------|-----------------------------------------|------------------|  
| `Status: ${trigger:payload.status:-unknown}` | `status` key is missing from payload.   | `Status: unknown`|  
| `Name: ${trigger:name:-guest}`             | `event.Name` is an empty string.        | `Name: guest`    |  
| `Time: ${trigger:timestamp:-now}`          | `event.Timestamp` is a zero time.       | `Time: now`      |  

If a field is not found and no default value is provided, the original placeholder is returned.
`Input`: Status: ${trigger:payload.status}  
`Result`: Status: ${trigger:payload.status}  
### Edge Cases 
`Invalid JSON Payload`: If the event payload is not valid JSON, any attempt to access a payload field (e.g., ${trigger:payload.user}) will fail, and the default value (or the original placeholder) will be used.  
`Nil Event`: If the event object passed to the Interpolate function is nil, all interpolations will use their default value.  
`Type Conversion`: All retrieved values, regardless of their original type (number, boolean, object, array), are converted to a string before being inserted into the final output. Objects and arrays are marshaled into their JSON string representation.  