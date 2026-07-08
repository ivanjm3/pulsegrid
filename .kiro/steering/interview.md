# Placement Interview Notes

Whenever a new feature or concept is implemented:

Ask yourself:

"What interview questions could be asked from this?"

Update:

docs/interview-notes.md

For every feature include:

## Feature

Example:
Presigned URL Generation

## Interview Questions

- Why use presigned URLs?
- How are they secured?
- What happens after expiration?
- Why not expose S3 publicly?
- What permissions are required?
- Difference between GET and PUT presigned URLs?

## Follow-up Questions

- How would you revoke one?
- Can they be reused?
- How would you audit usage?
- What attacks are possible?

## Resume Talking Points

- What engineering decision was made?
- Why was this implementation chosen?
- Alternatives considered.

Keep this file cumulative.
Do not overwrite previous notes.