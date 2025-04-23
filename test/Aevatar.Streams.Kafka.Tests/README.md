# Aevatar.Streams.Kafka.Tests

This project contains unit tests for the Aevatar.Streams.Kafka library, which provides Orleans streaming functionality for Kafka.

## Project Structure

- **Core/**: Tests for the core components of the Kafka adapter
  - `KafkaAdapterTests.cs`: Tests for the `KafkaAdapter` class
  - `KafkaAdapterReceiverTests.cs`: Tests for the `KafkaAdapterReceiver` class
- **TestFixture.cs**: Shared test fixture for common test setup

## Running Tests

To run the tests, use the following command from the solution root:

```bash
dotnet test
```

Or to run tests from this specific project:

```bash
dotnet test test/Aevatar.Streams.Kafka.Tests/Aevatar.Streams.Kafka.Tests.csproj
```

## Dependencies

The test project uses:
- xUnit as the test framework
- FluentAssertions for assertions
- Moq for mocking
- Orleans.TestingHost for Orleans testing utilities

## Test Implementation Notes

The tests in this project follow these practices:

1. **Mocking External Dependencies**: All external dependencies like `ILoggerFactory`, `IGrainFactory`, etc. are mocked using Moq.
2. **Fixture Pattern**: Common test setup is shared via the `TestFixture` class.
3. **Unit Testing Focus**: Tests focus on unit testing individual components in isolation.
4. **Testing Public API**: Tests primarily target the public API of the components.

## Known Issues and Limitations

- Some tests may require additional configuration due to the complex interaction with Kafka.
- The tests may need updating as the internal implementation of Kafka adapters evolves.
- For complete integration testing, a running Kafka instance would be required. 