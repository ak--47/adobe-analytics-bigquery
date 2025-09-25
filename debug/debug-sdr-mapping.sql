-- Debug SDR mapping issue
-- Check what's happening with the custom events mapping

-- First, check what's in the raw SDR table
SELECT 'Raw SDR Data' as source, Event, Name, LENGTH(Name) as name_length
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.sdr_custom_events_raw`
WHERE Event IN ('event5', 'event12', 'event14', 'event33')
ORDER BY Event;

-- Check what gets processed into the SDR events table
SELECT 'Processed SDR Data' as source, custom_event_number, name_override, code
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.sdr_custom_events`
WHERE custom_event_number IN (5, 12, 14, 33)
ORDER BY custom_event_number;

-- Check the final event mapping
SELECT 'Final Event Map' as source, code, name
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.event_map`
WHERE code IN (204, 211, 213, 232)
ORDER BY code;

-- Check base events lookup
SELECT 'Base Events' as source, id as code, name
FROM `mixpanel-gtm-training.korn_ferry_adobe_main.lookup_events`
WHERE id IN (204, 211, 213, 232)
ORDER BY id;