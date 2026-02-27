-- Migration 021: Add viewer role
-- The role column is TEXT; no DDL change needed.
-- This migration documents that 'viewer' is now a valid role value,
-- joining: admin, management, AE.
--
-- Viewer is the lowest privilege tier:
--   admin > management > AE > viewer
--
-- New Tailscale users are auto-provisioned with the viewer role.
-- Viewers can only access reporting & analytics pages.
-- Admins can upgrade viewers to AE/management via /users/<id>/edit.

-- no-op to satisfy migration runner
SELECT 1;
