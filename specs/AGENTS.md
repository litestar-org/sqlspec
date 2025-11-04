# Universal Agent Coordination Guide

This document provides a high-level overview of the agentic system in this project. For the complete, detailed context that governs agent behavior, see **`.gemini/GEMINI.md`**.

## The Workflow: From Idea to Archive

1.  **PRD (`/prd`)**: An idea is formalized into a plan.
2.  **Expert (`/implement`)**: The plan is turned into code, and the project's knowledge is updated in `specs/guides/`.
3.  **Testing (`/test`)**: The code is validated.
4.  **Review (`/review`)**: The knowledge and code are verified, and the workspace is archived.
5.  **Guides (`/sync-guides`)**: (As needed) The entire documentation is audited and synchronized with the codebase.
