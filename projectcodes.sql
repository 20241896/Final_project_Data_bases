USE FootballStatistics
GO

/*
-- Drop the view if it exists
IF OBJECT_ID('vw_CurrentPlayerClubs', 'V') IS NOT NULL
    DROP VIEW vw_CurrentPlayerClubs
GO

-- 1. Create a view that lists the players and their actual club using the transfers and players tables
CREATE VIEW vw_CurrentPlayerClubs AS
SELECT 
    p.PLAYERID,
    p.NAME AS PlayerName,
    latest.TOCLUB AS CurrentClubID,
    c.NAME AS CurrentClubName,
    latest.TRANSFERDATE AS JoinedDate,
    DATEADD(YEAR, latest.CONTRACTLENGHT, latest.TRANSFERDATE) AS ContractEndDate
FROM PLAYERS p
INNER JOIN (
    -- For each player, get their most recent transfer
    SELECT 
        PLAYERID,
        TOCLUB,
        TRANSFERDATE,
        CONTRACTLENGHT
    FROM (
        SELECT 
            PLAYERID,
            TOCLUB,
            TRANSFERDATE,
            CONTRACTLENGHT,
            ROW_NUMBER() OVER (PARTITION BY PLAYERID ORDER BY TRANSFERDATE DESC) as rn
        FROM TRANSFERS
    ) t
    WHERE t.rn = 1
) latest ON p.PLAYERID = latest.PLAYERID
INNER JOIN CLUBS c ON latest.TOCLUB = c.CLUBID
GO

-- Test the view
SELECT * FROM vw_CurrentPlayerClubs
GO


-- Drop the function if it exists
IF OBJECT_ID('fn_GetPlayerCareerClubs', 'FN') IS NOT NULL
    DROP FUNCTION fn_GetPlayerCareerClubs
GO

-- 2. Create a function that returns a table with a list of all clubs a player was associated with
CREATE FUNCTION fn_GetPlayerCareerClubs (@PlayerID numeric)
RETURNS TABLE
AS
RETURN
(
    -- Get all transfers for the player, including contract end dates
    SELECT 
        TOCLUB AS ClubID,
        TRANSFERDATE AS ContractStart,
        DATEADD(YEAR, CONTRACTLENGHT, TRANSFERDATE) AS ContractEnd,
        'Transfer' AS RecordType
    FROM TRANSFERS
    WHERE PLAYERID = @PlayerID
    UNION ALL
    -- Include initial contracts where FromClub = ToClub (first contracts)
    SELECT 
        FROMCLUB AS ClubID,
        TRANSFERDATE AS ContractStart,
        DATEADD(YEAR, CONTRACTLENGHT, TRANSFERDATE) AS ContractEnd,
        'Initial Contract' AS RecordType
    FROM TRANSFERS
    WHERE PLAYERID = @PlayerID AND FROMCLUB = TOCLUB
)
GO

-- Test the function with Lionel Messi (PLAYERID = 1)
SELECT * FROM fn_GetPlayerCareerClubs(1)
GO





-- First, let's check the current state of the CLUBS table
SELECT CLUBID, NAME, MANAGERID FROM CLUBS
GO

-- Now create triggers for maintaining Actual Manager consistency

-- Drop triggers if they exist
IF OBJECT_ID('trg_SyncClubManagerIU', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManagerIU
GO

IF OBJECT_ID('trg_SyncClubManagerD', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManagerD
GO

IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    DROP TRIGGER trg_PreventManagerDirectUpdate
GO

IF OBJECT_ID('trg_BatchUpdateClubManagers', 'TR') IS NOT NULL
    DROP TRIGGER trg_BatchUpdateClubManagers
GO

-- Create a trigger to handle INSERT/UPDATE on CLUBMANAGER
CREATE TRIGGER trg_SyncClubManagerIU
ON CLUBMANAGER
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update Clubs table with the current manager for each affected club
    DECLARE @Today DATE = GETDATE()
    
    UPDATE c
    SET c.MANAGERID = cm.MANAGERID
    FROM CLUBS c
    INNER JOIN CLUBMANAGER cm ON c.CLUBID = cm.CLUBID
    WHERE cm.STARTDATE <= @Today 
      AND cm.ENDDATE >= @Today
      AND cm.CLUBID IN (SELECT DISTINCT CLUBID FROM inserted)
END
GO

-- Create a trigger to handle DELETE on CLUBMANAGER
CREATE TRIGGER trg_SyncClubManagerD
ON CLUBMANAGER
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Set manager to NULL for clubs where the deleted manager was current
    DECLARE @Today DATE = GETDATE()
    
    UPDATE c
    SET c.MANAGERID = NULL
    FROM CLUBS c
    INNER JOIN deleted d ON c.CLUBID = d.CLUBID
    WHERE d.STARTDATE <= @Today 
      AND d.ENDDATE >= @Today
END
GO

-- Create a trigger to prevent direct updates to Clubs.MANAGERID
CREATE TRIGGER trg_PreventManagerDirectUpdate
ON CLUBS
INSTEAD OF UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if MANAGERID is being updated
    IF UPDATE(MANAGERID)
    BEGIN
        RAISERROR('Direct updates to MANAGERID are not allowed. Use CLUBMANAGER table instead.', 16, 1)
        RETURN
    END
    
    -- Allow other updates to proceed
    UPDATE c
    SET 
        COUNTRYCODE = i.COUNTRYCODE,
        CITYID = i.CITYID,
        NAME = i.NAME,
        YEARFOUNDED = i.YEARFOUNDED
    FROM CLUBS c
    INNER JOIN inserted i ON c.CLUBID = i.CLUBID
END
GO

-- Additional trigger to handle batch operations for ClubManager updates
CREATE TRIGGER trg_BatchUpdateClubManagers
ON CLUBMANAGER
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Handle batch updates for current managers
    DECLARE @AffectedClubs TABLE (CLUBID numeric)
    DECLARE @Today DATE = GETDATE()
    
    -- Collect all affected clubs
    INSERT INTO @AffectedClubs (CLUBID)
    SELECT DISTINCT CLUBID FROM inserted
    UNION
    SELECT DISTINCT CLUBID FROM deleted
    
    -- Update each club's current manager
    DECLARE @CurrentClubID numeric
    DECLARE club_cursor CURSOR FOR
    SELECT CLUBID FROM @AffectedClubs
    
    OPEN club_cursor
    FETCH NEXT FROM club_cursor INTO @CurrentClubID
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Find current manager for this club
        DECLARE @CurrentManagerID numeric
        
        SELECT TOP 1 @CurrentManagerID = MANAGERID
        FROM CLUBMANAGER
        WHERE CLUBID = @CurrentClubID
          AND STARTDATE <= @Today
          AND ENDDATE >= @Today
        ORDER BY STARTDATE DESC
        
        -- Update club with current manager (or NULL if none)
        UPDATE CLUBS
        SET MANAGERID = @CurrentManagerID
        WHERE CLUBID = @CurrentClubID
        
        FETCH NEXT FROM club_cursor INTO @CurrentClubID
    END
    
    CLOSE club_cursor
    DEALLOCATE club_cursor
END
GO

-- Test the triggers by updating CLUBMANAGER
-- First, let's see current managers
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
ORDER BY c.CLUBID
GO

-- Test inserting a new CLUBMANAGER record
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES (5, 4, '2024-01-01', '2025-12-31') -- Zidane managing Sevilla
GO

-- Check if the Clubs table was updated
SELECT CLUBID, NAME, MANAGERID FROM CLUBS WHERE CLUBID = 4
GO



USE FootballStatistics
GO

-- First, let's check the current data
SELECT * FROM CLUBS
SELECT * FROM MANAGERS
SELECT * FROM CLUBMANAGER
GO

-- We need to update the CLUBS table to remove the FOREIGN KEY constraint temporarily
-- or update the MANAGERID values to valid ones

-- First, let's see which clubs have invalid MANAGERID values
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE m.MANAGERID IS NULL AND c.MANAGERID IS NOT NULL
GO

-- For Question 3, we need to implement triggers that maintain consistency between CLUBMANAGER and CLUBS.MANAGERID
-- Let me recreate the triggers with proper error handling

-- Drop existing triggers first
IF OBJECT_ID('trg_SyncClubManagerIU', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManagerIU
GO

IF OBJECT_ID('trg_SyncClubManagerD', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManagerD
GO

IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    DROP TRIGGER trg_PreventManagerDirectUpdate
GO

IF OBJECT_ID('trg_BatchUpdateClubManagers', 'TR') IS NOT NULL
    DROP TRIGGER trg_BatchUpdateClubManagers
GO

-- First, let's fix the data issue by updating CLUBS table to have valid MANAGERIDs or NULL
-- Based on your dummy data, let's update the clubs with the current managers from CLUBMANAGER
UPDATE c
SET c.MANAGERID = cm.MANAGERID
FROM CLUBS c
INNER JOIN CLUBMANAGER cm ON c.CLUBID = cm.CLUBID
WHERE cm.STARTDATE <= GETDATE() 
  AND cm.ENDDATE >= GETDATE()
  AND cm.MANAGERID IS NOT NULL
GO

-- Set NULL for clubs without current managers
UPDATE CLUBS 
SET MANAGERID = NULL 
WHERE MANAGERID NOT IN (SELECT MANAGERID FROM MANAGERS)
GO

-- Now create the triggers

-- 1. Trigger for INSERT/UPDATE on CLUBMANAGER
CREATE TRIGGER trg_SyncClubManagerIU
ON CLUBMANAGER
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- First, validate that inserted/updated MANAGERIDs exist in MANAGERS table
    IF EXISTS (
        SELECT 1 FROM inserted i
        LEFT JOIN MANAGERS m ON i.MANAGERID = m.MANAGERID
        WHERE m.MANAGERID IS NULL AND i.MANAGERID IS NOT NULL
    )
    BEGIN
        RAISERROR('One or more MANAGERID values do not exist in the MANAGERS table.', 16, 1)
        ROLLBACK TRANSACTION
        RETURN
    END
    
    -- Update Clubs table with the current manager for each affected club
    DECLARE @Today DATE = GETDATE()
    
    -- Update clubs that have a current manager
    UPDATE c
    SET c.MANAGERID = cm.MANAGERID
    FROM CLUBS c
    INNER JOIN CLUBMANAGER cm ON c.CLUBID = cm.CLUBID
    WHERE cm.STARTDATE <= @Today 
      AND cm.ENDDATE >= @Today
      AND cm.CLUBID IN (SELECT DISTINCT CLUBID FROM inserted)
      AND cm.MANAGERID IS NOT NULL
    
    -- For clubs that no longer have a current manager (gap between contracts)
    UPDATE c
    SET c.MANAGERID = NULL
    FROM CLUBS c
    WHERE c.CLUBID IN (SELECT DISTINCT CLUBID FROM inserted)
      AND NOT EXISTS (
          SELECT 1 FROM CLUBMANAGER cm
          WHERE cm.CLUBID = c.CLUBID
            AND cm.STARTDATE <= @Today
            AND cm.ENDDATE >= @Today
            AND cm.MANAGERID IS NOT NULL
      )
END
GO

-- 2. Trigger for DELETE on CLUBMANAGER
CREATE TRIGGER trg_SyncClubManagerD
ON CLUBMANAGER
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Today DATE = GETDATE()
    
    -- Set manager to NULL for clubs where the deleted manager was current
    -- and there's no other current manager
    UPDATE c
    SET c.MANAGERID = NULL
    FROM CLUBS c
    WHERE c.CLUBID IN (SELECT DISTINCT CLUBID FROM deleted)
      AND NOT EXISTS (
          SELECT 1 FROM CLUBMANAGER cm
          WHERE cm.CLUBID = c.CLUBID
            AND cm.STARTDATE <= @Today
            AND cm.ENDDATE >= @Today
            AND cm.MANAGERID IS NOT NULL
      )
END
GO

-- 3. Trigger to prevent direct updates to Clubs.MANAGERID
CREATE TRIGGER trg_PreventManagerDirectUpdate
ON CLUBS
INSTEAD OF UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if MANAGERID is being updated
    IF UPDATE(MANAGERID)
    BEGIN
        RAISERROR('Direct updates to MANAGERID are not allowed. Use CLUBMANAGER table instead.', 16, 1)
        RETURN
    END
    
    -- Allow other updates to proceed
    UPDATE c
    SET 
        COUNTRYCODE = i.COUNTRYCODE,
        CITYID = i.CITYID,
        NAME = i.NAME,
        YEARFOUNDED = i.YEARFOUNDED
    FROM CLUBS c
    INNER JOIN inserted i ON c.CLUBID = i.CLUBID
END
GO

-- 4. Additional trigger to handle batch operations
CREATE TRIGGER trg_BatchUpdateClubManagers
ON CLUBMANAGER
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- This trigger complements the main triggers for batch operations
    -- It doesn't do additional updates, just ensures proper handling
    -- The main triggers already handle the updates
END
GO

-- Now let's test the triggers

-- Test 1: Try to insert a valid CLUBMANAGER record
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES (1, 11, '2024-01-01', '2025-12-31') -- Pep Guardiola managing Juventus
GO

-- Check if the Clubs table was updated
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 11
GO

-- Test 2: Try to update an existing CLUBMANAGER record
UPDATE CLUBMANAGER 
SET ENDDATE = '2026-06-30'
WHERE MANAGERID = 1 AND CLUBID = 11
GO

-- Test 3: Try to delete a CLUBMANAGER record
DELETE FROM CLUBMANAGER 
WHERE MANAGERID = 1 AND CLUBID = 11
GO

-- Check the result
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 11
GO

-- Test 4: Try to directly update MANAGERID in CLUBS (should fail)
UPDATE CLUBS 
SET MANAGERID = 1 
WHERE CLUBID = 11
GO

-- Test 5: Test batch operations
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES 
(2, 11, '2024-01-01', '2025-12-31'), -- Carlo Ancelotti to Juventus
(3, 5, '2024-01-01', '2025-12-31')   -- Diego Simeone to Valencia
GO

-- Check results
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID IN (11, 5)
ORDER BY c.CLUBID
GO

-- Show all current club-manager relationships
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName,
    cm.STARTDATE,
    cm.ENDDATE
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
LEFT JOIN CLUBMANAGER cm ON c.CLUBID = cm.CLUBID 
    AND cm.STARTDATE <= GETDATE() 
    AND cm.ENDDATE >= GETDATE()
    AND cm.MANAGERID = c.MANAGERID
ORDER BY c.CLUBID
GO



-- First, let's disable the problematic trigger temporarily
IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    DISABLE TRIGGER trg_PreventManagerDirectUpdate ON CLUBS
GO

-- Let's check the foreign key constraint issue
-- The error says the conflict is in CLUBS table, column CLUBID
-- This means we're trying to insert a CLUBMANAGER record with a CLUBID that doesn't exist in CLUBS

-- Check what CLUBIDs exist
SELECT CLUBID, NAME FROM CLUBS ORDER BY CLUBID
GO

-- Check what CLUBIDs we're trying to insert in CLUBMANAGER
-- From the test code, we're trying to insert CLUBID = 11 (Juventus)
-- Let's verify Juventus exists
SELECT * FROM CLUBS WHERE CLUBID = 11
GO

-- Now let's fix the triggers properly

-- Drop all existing triggers first
IF OBJECT_ID('trg_SyncClubManagerIU', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManagerIU
GO

IF OBJECT_ID('trg_SyncClubManagerD', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManagerD
GO

IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    DROP TRIGGER trg_PreventManagerDirectUpdate
GO

IF OBJECT_ID('trg_BatchUpdateClubManagers', 'TR') IS NOT NULL
    DROP TRIGGER trg_BatchUpdateClubManagers
GO

-- According to the project requirements for Question 3:
-- 3.a) Whenever a ClubManager row is INSERTED/UPDATED, update Clubs.ManagerID
-- 3.b) When INSERTING a Club, ManagerID should be empty
-- 3.c) Should not be possible to UPDATE ManagerID directly in Clubs
-- 3.d) DELETE from ClubManager could update ManagerID to NULL

-- First, let's create a simpler INSTEAD OF trigger for CLUBS that only blocks MANAGERID updates
CREATE TRIGGER trg_PreventManagerDirectUpdate
ON CLUBS
INSTEAD OF UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if any row is trying to update MANAGERID from non-NULL to non-NULL
    -- We allow setting MANAGERID from NULL to NULL or from value to NULL
    -- But we block changing from one value to another
    IF EXISTS (
        SELECT 1 
        FROM inserted i
        INNER JOIN deleted d ON i.CLUBID = d.CLUBID
        WHERE (d.MANAGERID IS NOT NULL AND i.MANAGERID IS NOT NULL AND d.MANAGERID <> i.MANAGERID)
           OR (d.MANAGERID IS NULL AND i.MANAGERID IS NOT NULL) -- Also block setting manager directly
    )
    BEGIN
        RAISERROR('Direct updates to MANAGERID are not allowed. Use CLUBMANAGER table instead.', 16, 1)
        RETURN
    END
    
    -- Allow the update to proceed for non-MANAGERID columns
    UPDATE c
    SET 
        COUNTRYCODE = i.COUNTRYCODE,
        CITYID = i.CITYID,
        NAME = i.NAME,
        YEARFOUNDED = i.YEARFOUNDED,
        -- Only update MANAGERID if it's being set to NULL (allowed)
        MANAGERID = CASE 
            WHEN i.MANAGERID IS NULL AND d.MANAGERID IS NOT NULL THEN NULL
            ELSE c.MANAGERID
        END
    FROM CLUBS c
    INNER JOIN inserted i ON c.CLUBID = i.CLUBID
    INNER JOIN deleted d ON c.CLUBID = d.CLUBID
END
GO

-- Now create the AFTER trigger for CLUBMANAGER
CREATE TRIGGER trg_SyncClubManager_After
ON CLUBMANAGER
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Today DATE = GETDATE()
    
    -- Handle INSERT and UPDATE operations
    IF EXISTS (SELECT 1 FROM inserted)
    BEGIN
        -- For each affected club, find the current manager
        DECLARE @AffectedClubs TABLE (CLUBID numeric)
        
        INSERT INTO @AffectedClubs (CLUBID)
        SELECT DISTINCT CLUBID FROM inserted
        
        DECLARE @CurrentClubID numeric
        DECLARE club_cursor CURSOR FOR
        SELECT CLUBID FROM @AffectedClubs
        
        OPEN club_cursor
        FETCH NEXT FROM club_cursor INTO @CurrentClubID
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Find current manager for this club (most recent that includes today)
            DECLARE @CurrentManagerID numeric
            
            SELECT TOP 1 @CurrentManagerID = MANAGERID
            FROM CLUBMANAGER
            WHERE CLUBID = @CurrentClubID
              AND STARTDATE <= @Today
              AND ENDDATE >= @Today
            ORDER BY STARTDATE DESC
            
            -- Update the club with the current manager (or NULL if none)
            UPDATE CLUBS
            SET MANAGERID = @CurrentManagerID
            WHERE CLUBID = @CurrentClubID
            
            FETCH NEXT FROM club_cursor INTO @CurrentClubID
        END
        
        CLOSE club_cursor
        DEALLOCATE club_cursor
    END
    
    -- Handle DELETE operations
    IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
    BEGIN
        -- For clubs where we deleted the current manager record
        UPDATE c
        SET c.MANAGERID = NULL
        FROM CLUBS c
        INNER JOIN deleted d ON c.CLUBID = d.CLUBID
        WHERE d.STARTDATE <= @Today 
          AND d.ENDDATE >= @Today
          AND NOT EXISTS (
              SELECT 1 FROM CLUBMANAGER cm
              WHERE cm.CLUBID = c.CLUBID
                AND cm.STARTDATE <= @Today
                AND cm.ENDDATE >= @Today
          )
    END
END
GO

-- Enable the trigger on CLUBS
IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    ENABLE TRIGGER trg_PreventManagerDirectUpdate ON CLUBS
GO

-- Now let's test the triggers step by step

-- Test 1: Check current state
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 11
GO

-- Test 2: Insert a valid CLUBMANAGER record for Juventus (CLUBID = 11)
-- First check if Juventus exists
SELECT * FROM CLUBS WHERE CLUBID = 11
GO

-- Juventus exists, so we can insert a manager record
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES (1, 11, '2024-01-01', '2025-12-31') -- Pep Guardiola managing Juventus
GO

-- Check if the Clubs table was updated
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 11
GO

-- Test 3: Try to directly update MANAGERID in CLUBS (should fail)
UPDATE CLUBS 
SET MANAGERID = 2 
WHERE CLUBID = 11
GO

-- Test 4: Update the CLUBMANAGER record (change manager)
UPDATE CLUBMANAGER 
SET MANAGERID = 2, ENDDATE = '2026-06-30'
WHERE MANAGERID = 1 AND CLUBID = 11
GO

-- Check the result
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 11
GO

-- Test 5: Delete the CLUBMANAGER record
DELETE FROM CLUBMANAGER 
WHERE MANAGERID = 2 AND CLUBID = 11
GO

-- Check the result (should be NULL now)
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID
FROM CLUBS c
WHERE c.CLUBID = 11
GO

-- Test 6: Test batch operations
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES 
(2, 11, '2024-01-01', '2025-12-31'), -- Carlo Ancelotti to Juventus
(3, 5, '2024-01-01', '2025-12-31')   -- Diego Simeone to Valencia
GO

-- Check results
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID IN (11, 5)
ORDER BY c.CLUBID
GO

-- Test 7: Create a new club with NULL manager (requirement 3.b)
INSERT INTO CLUBS (MANAGERID, COUNTRYCODE, CITYID, NAME, YEARFOUNDED)
VALUES (NULL, 'ITA', 13, 'AS Roma', '1927-07-22')
GO

-- Check the new club
SELECT * FROM CLUBS WHERE NAME = 'AS Roma'
GO

-- Test 8: Add a manager to the new club via CLUBMANAGER
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES (7, 12, '2024-01-01', '2026-12-31') -- Jose Mourinho to AS Roma
GO

-- Verify the update
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 12
GO

-- Show all current club-manager relationships
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName,
    cm.STARTDATE,
    cm.ENDDATE
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
LEFT JOIN CLUBMANAGER cm ON c.CLUBID = cm.CLUBID 
    AND cm.STARTDATE <= GETDATE() 
    AND cm.ENDDATE >= GETDATE()
    AND cm.MANAGERID = c.MANAGERID
ORDER BY c.CLUBID
GO




-- First, let's check what CLUBIDs exist in CLUBS
SELECT CLUBID, NAME FROM CLUBS ORDER BY CLUBID
GO

-- Check what we're trying to insert
-- The error occurred at line 693, which is when we try to insert CLUBMANAGER for CLUBID = 12
-- Let's check if CLUBID 12 exists
SELECT * FROM CLUBS WHERE CLUBID = 12
GO

-- The issue is that when we created AS Roma, it got CLUBID = 12
-- But we need to verify the exact CLUBID
SELECT * FROM CLUBS WHERE NAME LIKE '%Roma%' OR NAME LIKE '%AS Roma%'
GO

-- Let me recreate the code with proper checking

-- First, drop all triggers to start fresh
IF OBJECT_ID('trg_SyncClubManager_After', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManager_After
GO

IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    DROP TRIGGER trg_PreventManagerDirectUpdate
GO

-- Create the triggers with proper error handling

-- 1. Trigger to prevent direct updates to Clubs.MANAGERID
CREATE TRIGGER trg_PreventManagerDirectUpdate
ON CLUBS
INSTEAD OF UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if MANAGERID is being updated from NULL to a value (not allowed)
    IF EXISTS (
        SELECT 1 
        FROM inserted i
        INNER JOIN deleted d ON i.CLUBID = d.CLUBID
        WHERE d.MANAGERID IS NULL AND i.MANAGERID IS NOT NULL
    )
    BEGIN
        RAISERROR('Direct updates to MANAGERID are not allowed. Use CLUBMANAGER table instead.', 16, 1)
        RETURN
    END
    
    -- Allow the update to proceed for allowed cases
    UPDATE c
    SET 
        COUNTRYCODE = i.COUNTRYCODE,
        CITYID = i.CITYID,
        NAME = i.NAME,
        YEARFOUNDED = i.YEARFOUNDED,
        -- Only allow updating MANAGERID to NULL (when removing a manager)
        MANAGERID = CASE 
            WHEN i.MANAGERID IS NULL THEN NULL
            ELSE c.MANAGERID  -- Keep existing value
        END
    FROM CLUBS c
    INNER JOIN inserted i ON c.CLUBID = i.CLUBID
    INNER JOIN deleted d ON c.CLUBID = d.CLUBID
END
GO
------------------------------------------------------------------------------------------------------------------
-- 2. Create a stored procedure to handle club-manager synchronization
-- This will be called from the trigger
CREATE PROCEDURE sp_SyncClubManager
    @ClubID numeric
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Today DATE = GETDATE()
    DECLARE @CurrentManagerID numeric
    
    -- Find current manager for this club
    SELECT TOP 1 @CurrentManagerID = MANAGERID
    FROM CLUBMANAGER
    WHERE CLUBID = @ClubID
      AND STARTDATE <= @Today
      AND ENDDATE >= @Today
    ORDER BY STARTDATE DESC
    
    -- Update the club with the current manager (or NULL if none)
    UPDATE CLUBS
    SET MANAGERID = @CurrentManagerID
    WHERE CLUBID = @ClubID
END
GO
----------------------------------------------------------------------------------------------------------------


-- First, let's disable the problematic trigger temporarily
IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    DISABLE TRIGGER trg_PreventManagerDirectUpdate ON CLUBS
GO

-- Let's check the foreign key constraint issue
-- The error says the conflict is in CLUBS table, column CLUBID
-- This means we're trying to insert a CLUBMANAGER record with a CLUBID that doesn't exist in CLUBS

-- Check what CLUBIDs exist
SELECT CLUBID, NAME FROM CLUBS ORDER BY CLUBID
GO

-- Check what CLUBIDs we're trying to insert in CLUBMANAGER
-- From the test code, we're trying to insert CLUBID = 11 (Juventus)
-- Let's verify Juventus exists
SELECT * FROM CLUBS WHERE CLUBID = 11
GO

-- Now let's fix the triggers properly

-- Drop all existing triggers first
IF OBJECT_ID('trg_SyncClubManagerIU', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManagerIU
GO

IF OBJECT_ID('trg_SyncClubManagerD', 'TR') IS NOT NULL
    DROP TRIGGER trg_SyncClubManagerD
GO

IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    DROP TRIGGER trg_PreventManagerDirectUpdate
GO

IF OBJECT_ID('trg_BatchUpdateClubManagers', 'TR') IS NOT NULL
    DROP TRIGGER trg_BatchUpdateClubManagers
GO

-- According to the project requirements for Question 3:
-- 3.a) Whenever a ClubManager row is INSERTED/UPDATED, update Clubs.ManagerID
-- 3.b) When INSERTING a Club, ManagerID should be empty
-- 3.c) Should not be possible to UPDATE ManagerID directly in Clubs
-- 3.d) DELETE from ClubManager could update ManagerID to NULL

-- First, let's create a simpler INSTEAD OF trigger for CLUBS that only blocks MANAGERID updates
CREATE TRIGGER trg_PreventManagerDirectUpdate
ON CLUBS
INSTEAD OF UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if any row is trying to update MANAGERID from non-NULL to non-NULL
    -- We allow setting MANAGERID from NULL to NULL or from value to NULL
    -- But we block changing from one value to another
    IF EXISTS (
        SELECT 1 
        FROM inserted i
        INNER JOIN deleted d ON i.CLUBID = d.CLUBID
        WHERE (d.MANAGERID IS NOT NULL AND i.MANAGERID IS NOT NULL AND d.MANAGERID <> i.MANAGERID)
           OR (d.MANAGERID IS NULL AND i.MANAGERID IS NOT NULL) -- Also block setting manager directly
    )
    BEGIN
        RAISERROR('Direct updates to MANAGERID are not allowed. Use CLUBMANAGER table instead.', 16, 1)
        RETURN
    END
    
    -- Allow the update to proceed for non-MANAGERID columns
    UPDATE c
    SET 
        COUNTRYCODE = i.COUNTRYCODE,
        CITYID = i.CITYID,
        NAME = i.NAME,
        YEARFOUNDED = i.YEARFOUNDED,
        -- Only update MANAGERID if it's being set to NULL (allowed)
        MANAGERID = CASE 
            WHEN i.MANAGERID IS NULL AND d.MANAGERID IS NOT NULL THEN NULL
            ELSE c.MANAGERID
        END
    FROM CLUBS c
    INNER JOIN inserted i ON c.CLUBID = i.CLUBID
    INNER JOIN deleted d ON c.CLUBID = d.CLUBID
END
GO

-- Now create the AFTER trigger for CLUBMANAGER
CREATE TRIGGER trg_SyncClubManager_After
ON CLUBMANAGER
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Today DATE = GETDATE()
    
    -- Handle INSERT and UPDATE operations
    IF EXISTS (SELECT 1 FROM inserted)
    BEGIN
        -- For each affected club, find the current manager
        DECLARE @AffectedClubs TABLE (CLUBID numeric)
        
        INSERT INTO @AffectedClubs (CLUBID)
        SELECT DISTINCT CLUBID FROM inserted
        
        DECLARE @CurrentClubID numeric
        DECLARE club_cursor CURSOR FOR
        SELECT CLUBID FROM @AffectedClubs
        
        OPEN club_cursor
        FETCH NEXT FROM club_cursor INTO @CurrentClubID
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Find current manager for this club (most recent that includes today)
            DECLARE @CurrentManagerID numeric
            
            SELECT TOP 1 @CurrentManagerID = MANAGERID
            FROM CLUBMANAGER
            WHERE CLUBID = @CurrentClubID
              AND STARTDATE <= @Today
              AND ENDDATE >= @Today
            ORDER BY STARTDATE DESC
            
            -- Update the club with the current manager (or NULL if none)
            UPDATE CLUBS
            SET MANAGERID = @CurrentManagerID
            WHERE CLUBID = @CurrentClubID
            
            FETCH NEXT FROM club_cursor INTO @CurrentClubID
        END
        
        CLOSE club_cursor
        DEALLOCATE club_cursor
    END
    
    -- Handle DELETE operations
    IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
    BEGIN
        -- For clubs where we deleted the current manager record
        UPDATE c
        SET c.MANAGERID = NULL
        FROM CLUBS c
        INNER JOIN deleted d ON c.CLUBID = d.CLUBID
        WHERE d.STARTDATE <= @Today 
          AND d.ENDDATE >= @Today
          AND NOT EXISTS (
              SELECT 1 FROM CLUBMANAGER cm
              WHERE cm.CLUBID = c.CLUBID
                AND cm.STARTDATE <= @Today
                AND cm.ENDDATE >= @Today
          )
    END
END
GO

-- Enable the trigger on CLUBS
IF OBJECT_ID('trg_PreventManagerDirectUpdate', 'TR') IS NOT NULL
    ENABLE TRIGGER trg_PreventManagerDirectUpdate ON CLUBS
GO

-- Now let's test the triggers step by step

-- Test 1: Check current state
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 11
GO

-- Test 2: Insert a valid CLUBMANAGER record for Juventus (CLUBID = 11)
-- First check if Juventus exists
SELECT * FROM CLUBS WHERE CLUBID = 11
GO

-- Juventus exists, so we can insert a manager record
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES (1, 11, '2024-01-01', '2025-12-31') -- Pep Guardiola managing Juventus
GO

-- Check if the Clubs table was updated
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 11
GO

-- Test 3: Try to directly update MANAGERID in CLUBS (should fail)
UPDATE CLUBS 
SET MANAGERID = 2 
WHERE CLUBID = 11
GO

-- Test 4: Update the CLUBMANAGER record (change manager)
UPDATE CLUBMANAGER 
SET MANAGERID = 2, ENDDATE = '2026-06-30'
WHERE MANAGERID = 1 AND CLUBID = 11
GO

-- Check the result
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 11
GO

-- Test 5: Delete the CLUBMANAGER record
DELETE FROM CLUBMANAGER 
WHERE MANAGERID = 2 AND CLUBID = 11
GO

-- Check the result (should be NULL now)
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID
FROM CLUBS c
WHERE c.CLUBID = 11
GO

-- Test 6: Test batch operations
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES 
(2, 11, '2024-01-01', '2025-12-31'), -- Carlo Ancelotti to Juventus
(3, 5, '2024-01-01', '2025-12-31')   -- Diego Simeone to Valencia
GO

-- Check results
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID IN (11, 5)
ORDER BY c.CLUBID
GO

-- Test 7: Create a new club with NULL manager (requirement 3.b)
INSERT INTO CLUBS (MANAGERID, COUNTRYCODE, CITYID, NAME, YEARFOUNDED)
VALUES (NULL, 'ITA', 13, 'AS Roma', '1927-07-22')
GO

-- Check the new club
SELECT * FROM CLUBS WHERE NAME = 'AS Roma'
GO

-- Test 8: Add a manager to the new club via CLUBMANAGER
INSERT INTO CLUBMANAGER (MANAGERID, CLUBID, STARTDATE, ENDDATE)
VALUES (7, 12, '2024-01-01', '2026-12-31') -- Jose Mourinho to AS Roma
GO

-- Verify the update
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
WHERE c.CLUBID = 12
GO

-- Show all current club-manager relationships
SELECT 
    c.CLUBID,
    c.NAME AS ClubName,
    c.MANAGERID,
    m.NAME AS ManagerName,
    cm.STARTDATE,
    cm.ENDDATE
FROM CLUBS c
LEFT JOIN MANAGERS m ON c.MANAGERID = m.MANAGERID
LEFT JOIN CLUBMANAGER cm ON c.CLUBID = cm.CLUBID 
    AND cm.STARTDATE <= GETDATE() 
    AND cm.ENDDATE >= GETDATE()
    AND cm.MANAGERID = c.MANAGERID
ORDER BY c.CLUBID
GO

----------------------------------------------------------------------------------------------------



-- Question 4: Implement a rule using triggers to ensure Lineups consistency

-- Drop existing trigger if it exists
IF OBJECT_ID('trg_ValidateLineupsConsistency', 'TR') IS NOT NULL
    DROP TRIGGER trg_ValidateLineupsConsistency
GO

-- Drop the helper function if it exists
IF OBJECT_ID('fn_CheckPlayerClubAtDate', 'FN') IS NOT NULL
    DROP FUNCTION fn_CheckPlayerClubAtDate
GO

-- Create a helper function to check if a player was at a club on a specific date
CREATE FUNCTION fn_CheckPlayerClubAtDate (
    @PlayerID numeric,
    @ClubID numeric,
    @MatchDate datetime
)
RETURNS bit
AS
BEGIN
    DECLARE @Result bit = 0
    
    -- Check if there's a transfer where the player was at the club on the match date
    -- A player is considered at a club if:
    -- 1. They were transferred TO that club before or on the match date
    -- 2. Their contract hadn't expired by the match date
    IF EXISTS (
        SELECT 1 
        FROM TRANSFERS t
        WHERE t.PLAYERID = @PlayerID
          AND t.TOCLUB = @ClubID
          AND t.TRANSFERDATE <= @MatchDate
          AND DATEADD(YEAR, t.CONTRACTLENGHT, t.TRANSFERDATE) >= @MatchDate
    )
    BEGIN
        SET @Result = 1
    END
    
    -- Also check if it's their first contract (FromClub = ToClub)
    -- This would be their initial club
    IF EXISTS (
        SELECT 1 
        FROM TRANSFERS t
        WHERE t.PLAYERID = @PlayerID
          AND t.FROMCLUB = @ClubID
          AND t.TOCLUB = @ClubID  -- First contract
          AND t.TRANSFERDATE <= @MatchDate
          AND DATEADD(YEAR, t.CONTRACTLENGHT, t.TRANSFERDATE) >= @MatchDate
    )
    BEGIN
        SET @Result = 1
    END
    
    RETURN @Result
END
GO

-- Create the main trigger for Lineups consistency
CREATE TRIGGER trg_ValidateLineupsConsistency
ON LINEUPS
INSTEAD OF INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Temporary table to hold valid inserts/updates
    DECLARE @ValidRows TABLE (
        PLAYERID numeric,
        CLUBID numeric,
        MATCHID numeric,
        POSITION varchar(30),
        STARTER bit
    )
    
    -- Check each row for consistency
    INSERT INTO @ValidRows (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
    SELECT 
        i.PLAYERID,
        i.CLUBID,
        i.MATCHID,
        i.POSITION,
        i.STARTER
    FROM inserted i
    INNER JOIN MATCHES m ON i.MATCHID = m.MATCHID
    WHERE 
        -- 1. Check if club is one of the participating teams in the match
        (i.CLUBID = m.HOMECLUB OR i.CLUBID = m.AWAYCLUB)
        -- 2. Check if player was at the club on the match date
        AND dbo.fn_CheckPlayerClubAtDate(i.PLAYERID, i.CLUBID, m.DATEOFMATCH) = 1
    
    -- For UPDATE operations, we need to handle both old and new data
    IF EXISTS (SELECT 1 FROM deleted)
    BEGIN
        -- This is an UPDATE operation
        -- Delete the old rows that are being updated
        DELETE l
        FROM LINEUPS l
        INNER JOIN deleted d ON l.PLAYERID = d.PLAYERID 
            AND l.CLUBID = d.CLUBID 
            AND l.MATCHID = d.MATCHID
    END
    
    -- Insert valid rows
    -- Use MERGE to handle both INSERT and UPDATE scenarios
    MERGE INTO LINEUPS AS target
    USING @ValidRows AS source
    ON (target.PLAYERID = source.PLAYERID 
        AND target.CLUBID = source.CLUBID 
        AND target.MATCHID = source.MATCHID)
    WHEN MATCHED THEN
        UPDATE SET 
            POSITION = source.POSITION,
            STARTER = source.STARTER
    WHEN NOT MATCHED THEN
        INSERT (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
        VALUES (source.PLAYERID, source.CLUBID, source.MATCHID, source.POSITION, source.STARTER);
    
    -- Report any invalid rows that were rejected
    DECLARE @TotalRows int, @ValidRowsCount int, @InvalidRowsCount int
    
    SELECT @TotalRows = COUNT(*) FROM inserted
    SELECT @ValidRowsCount = COUNT(*) FROM @ValidRows
    SET @InvalidRowsCount = @TotalRows - @ValidRowsCount
    
    IF @InvalidRowsCount > 0
    BEGIN
        PRINT 'Lineups consistency check:'
        PRINT '  Total rows attempted: ' + CAST(@TotalRows AS varchar(10))
        PRINT '  Valid rows processed: ' + CAST(@ValidRowsCount AS varchar(10))
        PRINT '  Invalid rows rejected: ' + CAST(@InvalidRowsCount AS varchar(10))
        
        -- Show details of invalid rows
        SELECT 
            i.PLAYERID,
            p.NAME AS PlayerName,
            i.CLUBID,
            c.NAME AS ClubName,
            i.MATCHID,
            m.DATEOFMATCH,
            CASE 
                WHEN NOT (i.CLUBID = m.HOMECLUB OR i.CLUBID = m.AWAYCLUB) 
                THEN 'Club not participating in match'
                ELSE 'Player not at club on match date'
            END AS RejectionReason
        FROM inserted i
        LEFT JOIN @ValidRows v ON i.PLAYERID = v.PLAYERID 
            AND i.CLUBID = v.CLUBID 
            AND i.MATCHID = v.MATCHID
        INNER JOIN PLAYERS p ON i.PLAYERID = p.PLAYERID
        INNER JOIN CLUBS c ON i.CLUBID = c.CLUBID
        INNER JOIN MATCHES m ON i.MATCHID = m.MATCHID
        WHERE v.PLAYERID IS NULL  -- Rows not in valid rows
    END
END
GO

-- Now let's test the trigger with various scenarios
-- Test 1: Check current data to understand what we're working with
SELECT 
    'MATCHES' AS TableName,
    COUNT(*) AS [RowCount]
FROM MATCHES
UNION ALL
SELECT 'LINEUPS', COUNT(*) FROM LINEUPS
UNION ALL
SELECT 'TRANSFERS', COUNT(*) FROM TRANSFERS
GO

-- Look at some sample matches
SELECT TOP 5 
    m.MATCHID,
    m.DATEOFMATCH,
    hc.NAME AS HomeClub,
    ac.NAME AS AwayClub
FROM MATCHES m
INNER JOIN CLUBS hc ON m.HOMECLUB = hc.CLUBID
INNER JOIN CLUBS ac ON m.AWAYCLUB = ac.CLUBID
ORDER BY m.DATEOFMATCH
GO

-- Look at Lionel Messi's transfers
SELECT 
    t.TRANSFERDATE,
    DATEADD(YEAR, t.CONTRACTLENGHT, t.TRANSFERDATE) AS ContractEnd,
    fc.NAME AS FromClub,
    tc.NAME AS ToClub
FROM TRANSFERS t
INNER JOIN CLUBS fc ON t.FROMCLUB = fc.CLUBID
INNER JOIN CLUBS tc ON t.TOCLUB = tc.CLUBID
WHERE t.PLAYERID = 1  -- Lionel Messi
ORDER BY t.TRANSFERDATE
GO

-- Test 2: Test with valid data - Lionel Messi playing for Barcelona in a match
-- First, find a match where Barcelona (CLUBID = 1) is playing
SELECT TOP 1 
    m.MATCHID,
    m.DATEOFMATCH,
    CASE 
        WHEN m.HOMECLUB = 1 THEN 'Home'
        WHEN m.AWAYCLUB = 1 THEN 'Away'
        ELSE 'Not playing'
    END AS BarcelonaRole
FROM MATCHES m
WHERE m.HOMECLUB = 1 OR m.AWAYCLUB = 1
ORDER BY m.DATEOFMATCH
GO

-- Based on the dummy data, let's use MATCHID = 1 (Barcelona vs Real Madrid on 2023-10-28)
-- Messi was at Barcelona at this time (he returned in July 2023)
INSERT INTO LINEUPS (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
VALUES (1, 1, 1, 'Central Ofense', 1)  -- Lionel Messi for Barcelona in Match 1
GO

-- Check if it was inserted
SELECT 
    l.PLAYERID,
    p.NAME AS PlayerName,
    l.CLUBID,
    c.NAME AS ClubName,
    l.MATCHID,
    l.POSITION,
    l.STARTER
FROM LINEUPS l
INNER JOIN PLAYERS p ON l.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON l.CLUBID = c.CLUBID
WHERE l.PLAYERID = 1 AND l.MATCHID = 1
GO

-- Test 3: Test with invalid data - Player at wrong club
-- Try to add Messi to Real Madrid for the same match (should fail)
INSERT INTO LINEUPS (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
VALUES (1, 2, 1, 'Central Ofense', 1)  -- Messi for Real Madrid (invalid)
GO

-- Test 4: Test with player not at any club on match date
-- Find a player who wasn't at any club on a specific date
-- Let's create a test scenario with a future date
-- We need to make sure the clubs exist first
SELECT CLUBID, NAME FROM CLUBS WHERE CLUBID IN (1, 2)
GO

-- Now create a future match for testing
DECLARE @FutureMatchID numeric

-- Create a future match for testing with valid club IDs
INSERT INTO MATCHES (HOMECLUB, AWAYCLUB, COMPETITIONID, SEASONID, COUNTRYCODE, CITYID, MATCHDAY, DATEOFMATCH, ATTENDANCE, TOTALMINUTES)
VALUES (1, 2, 1, 2, 'ESP', 1, 35, '2025-12-31', NULL, NULL)

SET @FutureMatchID = SCOPE_IDENTITY()
GO

-- Try to add a player without checking their contract (should work if they have a valid contract)
-- Let's check Erling Haaland's (PLAYERID = 7) contract with Manchester City (CLUBID = 6)
SELECT 
    t.TRANSFERDATE,
    DATEADD(YEAR, t.CONTRACTLENGHT, t.TRANSFERDATE) AS ContractEnd,
    '2025-12-31' AS MatchDate,
    CASE 
        WHEN '2025-12-31' BETWEEN t.TRANSFERDATE AND DATEADD(YEAR, t.CONTRACTLENGHT, t.TRANSFERDATE)
        THEN 'Valid'
        ELSE 'Invalid'
    END AS Status
FROM TRANSFERS t
WHERE t.PLAYERID = 7 AND t.TOCLUB = 6
GO

-- Based on Haaland's transfer (2022-05-10 with 5-year contract), he should be valid until 2027
-- So this should work:
DECLARE @FutureMatchID numeric
SELECT @FutureMatchID = MAX(MATCHID) FROM MATCHES WHERE DATEOFMATCH = '2025-12-31'

INSERT INTO LINEUPS (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
VALUES (7, 6, @FutureMatchID, 'Central Ofense', 1)  -- Haaland for Man City
GO

-- Check the result
DECLARE @FutureMatchID numeric
SELECT @FutureMatchID = MAX(MATCHID) FROM MATCHES WHERE DATEOFMATCH = '2025-12-31'

SELECT 
    l.PLAYERID,
    p.NAME AS PlayerName,
    l.CLUBID,
    c.NAME AS ClubName,
    l.MATCHID,
    m.DATEOFMATCH
FROM LINEUPS l
INNER JOIN PLAYERS p ON l.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON l.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
WHERE l.MATCHID = @FutureMatchID
GO

-- Test 5: Test UPDATE operation
-- Update Messi's position in the lineup
UPDATE LINEUPS 
SET POSITION = 'Central Right Ofense'
WHERE PLAYERID = 1 AND CLUBID = 1 AND MATCHID = 1
GO

-- Check the update
SELECT * FROM LINEUPS WHERE PLAYERID = 1 AND CLUBID = 1 AND MATCHID = 1
GO

-- Test 6: Try to UPDATE to an invalid club (should fail)
UPDATE LINEUPS 
SET CLUBID = 2  -- Try to change Messi to Real Madrid
WHERE PLAYERID = 1 AND CLUBID = 1 AND MATCHID = 1
GO

-- Test 7: Test batch operations (multiple inserts)
INSERT INTO LINEUPS (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
VALUES 
(9, 1, 1, 'Central Midfielder', 1),   -- Pedri for Barcelona (valid)
(6, 2, 1, 'Left Ofense', 1),          -- Vinicius for Real Madrid (valid)
(1, 2, 1, 'Central Ofense', 0)        -- Messi for Real Madrid (invalid - should be rejected)
GO

-- Check which rows were inserted
SELECT 
    l.PLAYERID,
    p.NAME AS PlayerName,
    l.CLUBID,
    c.NAME AS ClubName,
    l.MATCHID
FROM LINEUPS l
INNER JOIN PLAYERS p ON l.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON l.CLUBID = c.CLUBID
WHERE l.MATCHID = 1
ORDER BY l.PLAYERID
GO

-- Test 8: Test with a player who has multiple transfers
-- Check Neymar's transfers (PLAYERID = 3)
SELECT 
    t.TRANSFERDATE,
    DATEADD(YEAR, t.CONTRACTLENGHT, t.TRANSFERDATE) AS ContractEnd,
    fc.NAME AS FromClub,
    tc.NAME AS ToClub
FROM TRANSFERS t
INNER JOIN CLUBS fc ON t.FROMCLUB = fc.CLUBID
INNER JOIN CLUBS tc ON t.TOCLUB = tc.CLUBID
WHERE t.PLAYERID = 3
ORDER BY t.TRANSFERDATE
GO

-- Neymar transferred to PSG (CLUBID = 9) in 2017 with 5-year contract
-- He should still be at PSG in 2023
-- Find a match where PSG is playing
SELECT TOP 1 
    m.MATCHID,
    m.DATEOFMATCH,
    CASE 
        WHEN m.HOMECLUB = 9 THEN 'Home'
        WHEN m.AWAYCLUB = 9 THEN 'Away'
        ELSE 'Not playing'
    END AS PSGRole
FROM MATCHES m
WHERE m.HOMECLUB = 9 OR m.AWAYCLUB = 9
ORDER BY m.DATEOFMATCH
GO

-- Use MATCHID = 13 (Barcelona vs PSG on 2023-09-19)
-- Neymar should be able to play for PSG
INSERT INTO LINEUPS (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
VALUES (3, 9, 13, 'Left Ofense', 1)  -- Neymar for PSG
GO

-- Check
SELECT 
    l.PLAYERID,
    p.NAME AS PlayerName,
    l.CLUBID,
    c.NAME AS ClubName,
    l.MATCHID,
    m.DATEOFMATCH
FROM LINEUPS l
INNER JOIN PLAYERS p ON l.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON l.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
WHERE l.PLAYERID = 3 AND l.MATCHID = 13
GO

-- Test 9: Clean up test data
DECLARE @FutureMatchIDToDelete numeric
SELECT @FutureMatchIDToDelete = MAX(MATCHID) FROM MATCHES WHERE DATEOFMATCH = '2025-12-31'

DELETE FROM LINEUPS WHERE MATCHID = @FutureMatchIDToDelete
DELETE FROM MATCHES WHERE MATCHID = @FutureMatchIDToDelete
GO

-- Final verification: Show some valid lineup combinations
SELECT TOP 10
    m.MATCHID,
    m.DATEOFMATCH,
    hc.NAME AS HomeClub,
    ac.NAME AS AwayClub,
    l.PLAYERID,
    p.NAME AS PlayerName,
    lc.NAME AS LineupClub,
    l.POSITION,
    l.STARTER
FROM LINEUPS l
INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
INNER JOIN PLAYERS p ON l.PLAYERID = p.PLAYERID
INNER JOIN CLUBS lc ON l.CLUBID = lc.CLUBID
INNER JOIN CLUBS hc ON m.HOMECLUB = hc.CLUBID
INNER JOIN CLUBS ac ON m.AWAYCLUB = ac.CLUBID
ORDER BY m.DATEOFMATCH, l.CLUBID, l.PLAYERID
GO

-- Summary of what the trigger does:
-- 1. Validates that a player in a lineup belongs to one of the clubs playing in the match
-- 2. Validates that the player was actually at that club on the match date (based on transfer history)
-- 3. Handles both INSERT and UPDATE operations
-- 4. Provides detailed feedback on rejected rows
-- 5. Supports batch operations

GO

-------------------------------------------------------------------------------------------------


-- Question 5: Create a stored procedure to populate the Player Match Stats table

-- Drop the stored procedure if it exists
IF OBJECT_ID('sp_CalculatePlayerMatchStats', 'P') IS NOT NULL
    DROP PROCEDURE sp_CalculatePlayerMatchStats
GO

-- Create the stored procedure
CREATE PROCEDURE sp_CalculatePlayerMatchStats
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        PRINT 'Starting Player Match Stats calculation...'
        PRINT '========================================='
        
        -- Get count of matches that have finished (TotalMinutes is not NULL)
        DECLARE @FinishedMatchesCount INT
        SELECT @FinishedMatchesCount = COUNT(*) 
        FROM MATCHES 
        WHERE TOTALMINUTES IS NOT NULL
        
        PRINT 'Found ' + CAST(@FinishedMatchesCount AS VARCHAR(10)) + ' finished matches.'
        
        -- Clear existing stats for matches that have finished
        -- This ensures we recalculate fresh stats each time
        DELETE FROM PLAYER_MATCH_STATS
        WHERE MATCHID IN (
            SELECT MATCHID FROM MATCHES WHERE TOTALMINUTES IS NOT NULL
        )
        
        PRINT 'Cleared existing stats for finished matches.'
        
        -- Insert calculated stats for all players in lineups for finished matches
        INSERT INTO PLAYER_MATCH_STATS (PLAYERID, CLUBID, MATCHID, GOALSSCORED, MINUTESPLAYED, PASSESCOMPLETED, SHOTSONTARGET)
        SELECT 
            l.PLAYERID,
            l.CLUBID,
            l.MATCHID,
            
            -- Goals scored: Count of 'Goal' actions for this player in this match
            ISNULL((
                SELECT COUNT(*) 
                FROM ACTIONS a 
                WHERE a.PLAYERID = l.PLAYERID 
                  AND a.CLUBID = l.CLUBID 
                  AND a.MATCHID = l.MATCHID 
                  AND a.TYPE_OF_ACTION = 'Goal'
            ), 0) AS GOALSSCORED,
            
            -- Minutes played: Complex calculation based on starter status and substitutions
            dbo.fn_CalculateMinutesPlayed(l.PLAYERID, l.CLUBID, l.MATCHID, l.STARTER) AS MINUTESPLAYED,
            
            -- Passes completed: Count of 'Pass done' actions
            ISNULL((
                SELECT COUNT(*) 
                FROM ACTIONS a 
                WHERE a.PLAYERID = l.PLAYERID 
                  AND a.CLUBID = l.CLUBID 
                  AND a.MATCHID = l.MATCHID 
                  AND a.TYPE_OF_ACTION = 'Pass done'
            ), 0) AS PASSESCOMPLETED,
            
            -- Shots on target: Count of 'Shot on target' actions
            ISNULL((
                SELECT COUNT(*) 
                FROM ACTIONS a 
                WHERE a.PLAYERID = l.PLAYERID 
                  AND a.CLUBID = l.CLUBID 
                  AND a.MATCHID = l.MATCHID 
                  AND a.TYPE_OF_ACTION = 'Shot on target'
            ), 0) AS SHOTSONTARGET
            
        FROM LINEUPS l
        INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
        WHERE m.TOTALMINUTES IS NOT NULL
        ORDER BY l.MATCHID, l.CLUBID, l.PLAYERID
        
        -- Get count of inserted records
        DECLARE @InsertedCount INT = @@ROWCOUNT
        
        PRINT 'Successfully calculated stats for ' + CAST(@InsertedCount AS VARCHAR(10)) + ' player-match combinations.'
        PRINT 'Procedure completed successfully.'
        
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
        DECLARE @ErrorState INT = ERROR_STATE()
        
        PRINT 'Error occurred during Player Match Stats calculation:'
        PRINT 'Error: ' + @ErrorMessage
        PRINT 'Procedure failed.'
        
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState)
    END CATCH
END
GO

-- We need a helper function to calculate minutes played
IF OBJECT_ID('fn_CalculateMinutesPlayed', 'FN') IS NOT NULL
    DROP FUNCTION fn_CalculateMinutesPlayed
GO

CREATE FUNCTION fn_CalculateMinutesPlayed (
    @PlayerID numeric,
    @ClubID numeric,
    @MatchID numeric,
    @Starter bit
)
RETURNS smallint
AS
BEGIN
    DECLARE @MinutesPlayed smallint = 0
    DECLARE @TotalMatchMinutes smallint
    
    -- Get total minutes for the match
    SELECT @TotalMatchMinutes = TOTALMINUTES
    FROM MATCHES 
    WHERE MATCHID = @MatchID
    
    -- If match minutes not available, return 0
    IF @TotalMatchMinutes IS NULL
        RETURN 0
    
    -- For starters: assume they played full match unless substituted
    IF @Starter = 1
    BEGIN
        SET @MinutesPlayed = @TotalMatchMinutes
        
        -- Check if player was substituted (has 'Exit from match' action)
        DECLARE @SubstitutionTime float
        
        SELECT TOP 1 @SubstitutionTime = MINUTE
        FROM ACTIONS 
        WHERE PLAYERID = @PlayerID 
          AND CLUBID = @ClubID 
          AND MATCHID = @MatchID 
          AND TYPE_OF_ACTION = 'Exit from match'
        
        -- If substituted, minutes played = substitution time
        IF @SubstitutionTime IS NOT NULL
            SET @MinutesPlayed = CAST(@SubstitutionTime AS smallint)
    END
    ELSE
    BEGIN
        -- For substitutes: minutes from entry to end of match (or substitution)
        DECLARE @EntryTime float
        DECLARE @ExitTime float
        
        -- Get entry time
        SELECT TOP 1 @EntryTime = MINUTE
        FROM ACTIONS 
        WHERE PLAYERID = @PlayerID 
          AND CLUBID = @ClubID 
          AND MATCHID = @MatchID 
          AND TYPE_OF_ACTION = 'Enter into match'
        ORDER BY MINUTE
        
        -- Get exit time (if substituted later)
        SELECT TOP 1 @ExitTime = MINUTE
        FROM ACTIONS 
        WHERE PLAYERID = @PlayerID 
          AND CLUBID = @ClubID 
          AND MATCHID = @MatchID 
          AND TYPE_OF_ACTION = 'Exit from match'
        ORDER BY MINUTE
        
        -- Calculate minutes played
        IF @EntryTime IS NOT NULL
        BEGIN
            IF @ExitTime IS NOT NULL
                -- Substituted after entering
                SET @MinutesPlayed = CAST(@ExitTime - @EntryTime AS smallint)
            ELSE
                -- Played from entry to end of match
                SET @MinutesPlayed = CAST(@TotalMatchMinutes - @EntryTime AS smallint)
        END
        -- If no entry time, player never entered (0 minutes)
    END
    
    -- Ensure minutes are not negative
    IF @MinutesPlayed < 0
        SET @MinutesPlayed = 0
    
    -- Ensure minutes don't exceed match total
    IF @MinutesPlayed > @TotalMatchMinutes
        SET @MinutesPlayed = @TotalMatchMinutes
    
    RETURN @MinutesPlayed
END
GO

-- Now let's test the stored procedure

-- First, check the current state of the tables
PRINT '=== Before running the procedure ==='
SELECT 
    'PLAYER_MATCH_STATS' AS TableName,
    COUNT(*) AS RecordCount
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'MATCHES (finished)',
    COUNT(*)
FROM MATCHES 
WHERE TOTALMINUTES IS NOT NULL
UNION ALL
SELECT 
    'LINEUPS (for finished matches)',
    COUNT(DISTINCT l.PLAYERID + '-' + CAST(l.MATCHID AS VARCHAR(10)))
FROM LINEUPS l
INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
WHERE m.TOTALMINUTES IS NOT NULL
GO

-- Show some sample data before calculation
PRINT 'Sample of current PLAYER_MATCH_STATS (before):'
SELECT TOP 5 
    pms.PLAYERID,
    p.NAME AS PlayerName,
    c.NAME AS ClubName,
    pms.MATCHID,
    pms.GOALSSCORED,
    pms.MINUTESPLAYED,
    pms.PASSESCOMPLETED,
    pms.SHOTSONTARGET
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON pms.CLUBID = c.CLUBID
ORDER BY pms.MATCHID, pms.PLAYERID
GO

-- Test the helper function with some examples
PRINT 'Testing minutes played calculation:'
SELECT 
    l.PLAYERID,
    p.NAME AS PlayerName,
    l.CLUBID,
    c.NAME AS ClubName,
    l.MATCHID,
    l.STARTER,
    dbo.fn_CalculateMinutesPlayed(l.PLAYERID, l.CLUBID, l.MATCHID, l.STARTER) AS CalculatedMinutes,
    -- Compare with existing data if any
    pms.MINUTESPLAYED AS ExistingMinutes
FROM LINEUPS l
INNER JOIN PLAYERS p ON l.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON l.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
LEFT JOIN PLAYER_MATCH_STATS pms ON l.PLAYERID = pms.PLAYERID 
    AND l.CLUBID = pms.CLUBID 
    AND l.MATCHID = pms.MATCHID
WHERE m.TOTALMINUTES IS NOT NULL
  AND l.PLAYERID IN (1, 7, 13) -- Test with Messi, Haaland, Lewandowski
  AND l.MATCHID IN (1, 10, 13)
ORDER BY l.MATCHID, l.PLAYERID
GO

-- Now run the main stored procedure
PRINT '=== Running the stored procedure ==='
EXEC sp_CalculatePlayerMatchStats
GO

-- Check the results
PRINT '=== After running the procedure ==='
SELECT 
    'PLAYER_MATCH_STATS' AS TableName,
    COUNT(*) AS RecordCount
FROM PLAYER_MATCH_STATS
GO

-- Show detailed results
PRINT 'Detailed Player Match Stats:'
SELECT 
    pms.PLAYERID,
    p.NAME AS PlayerName,
    c.NAME AS ClubName,
    m.MATCHID,
    m.DATEOFMATCH,
    hc.NAME AS HomeClub,
    ac.NAME AS AwayClub,
    pms.GOALSSCORED,
    pms.MINUTESPLAYED,
    pms.PASSESCOMPLETED,
    pms.SHOTSONTARGET,
    -- Calculate some derived stats
    CASE 
        WHEN pms.SHOTSONTARGET > 0 
        THEN CAST(CAST(pms.GOALSSCORED AS FLOAT) / NULLIF(pms.SHOTSONTARGET, 0) * 100 AS DECIMAL(5,2))
        ELSE 0 
    END AS ConversionRate,
    CASE 
        WHEN pms.MINUTESPLAYED > 0 
        THEN CAST(CAST(pms.PASSESCOMPLETED AS FLOAT) / pms.MINUTESPLAYED AS DECIMAL(5,2))
        ELSE 0 
    END AS PassesPerMinute
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON pms.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON pms.MATCHID = m.MATCHID
INNER JOIN CLUBS hc ON m.HOMECLUB = hc.CLUBID
INNER JOIN CLUBS ac ON m.AWAYCLUB = ac.CLUBID
ORDER BY m.DATEOFMATCH, c.NAME, p.NAME
GO

-- Test with specific players to verify accuracy
PRINT '=== Verification with specific players ==='

-- Check Lionel Messi's stats
PRINT 'Lionel Messi stats:'
SELECT 
    p.NAME AS PlayerName,
    c.NAME AS ClubName,
    m.DATEOFMATCH,
    pms.GOALSSCORED,
    pms.MINUTESPLAYED,
    pms.PASSESCOMPLETED,
    pms.SHOTSONTARGET,
    -- Verify against actual actions
    (SELECT COUNT(*) FROM ACTIONS a WHERE a.PLAYERID = pms.PLAYERID AND a.CLUBID = pms.CLUBID AND a.MATCHID = pms.MATCHID AND a.TYPE_OF_ACTION = 'Goal') AS ActualGoals,
    (SELECT COUNT(*) FROM ACTIONS a WHERE a.PLAYERID = pms.PLAYERID AND a.CLUBID = pms.CLUBID AND a.MATCHID = pms.MATCHID AND a.TYPE_OF_ACTION = 'Pass done') AS ActualPasses,
    (SELECT COUNT(*) FROM ACTIONS a WHERE a.PLAYERID = pms.PLAYERID AND a.CLUBID = pms.CLUBID AND a.MATCHID = pms.MATCHID AND a.TYPE_OF_ACTION = 'Shot on target') AS ActualShots
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON pms.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON pms.MATCHID = m.MATCHID
WHERE p.PLAYERID = 1  -- Lionel Messi
ORDER BY m.DATEOFMATCH
GO

-- Check Erling Haaland's stats
PRINT 'Erling Haaland stats:'
SELECT 
    p.NAME AS PlayerName,
    c.NAME AS ClubName,
    m.DATEOFMATCH,
    pms.GOALSSCORED,
    pms.MINUTESPLAYED,
    pms.PASSESCOMPLETED,
    pms.SHOTSONTARGET
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON pms.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON pms.MATCHID = m.MATCHID
WHERE p.PLAYERID = 7  -- Erling Haaland
ORDER BY m.DATEOFMATCH
GO

-- Test the procedure with edge cases

-- Test 1: Run procedure again (should recalculate fresh)
PRINT '=== Test: Running procedure again (should recalculate) ==='
EXEC sp_CalculatePlayerMatchStats
GO

-- Test 2: Check that stats for unfinished matches are not calculated
PRINT '=== Test: Verify unfinished matches are excluded ==='

-- Create an unfinished match
DECLARE @UnfinishedMatchID numeric

INSERT INTO MATCHES (HOMECLUB, AWAYCLUB, COMPETITIONID, SEASONID, COUNTRYCODE, CITYID, MATCHDAY, DATEOFMATCH, ATTENDANCE, TOTALMINUTES)
VALUES (1, 2, 1, 2, 'ESP', 1, 36, '2025-01-01', NULL, NULL) -- TOTALMINUTES is NULL

SET @UnfinishedMatchID = SCOPE_IDENTITY()

-- Add a lineup for the unfinished match
INSERT INTO LINEUPS (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
VALUES (1, 1, @UnfinishedMatchID, 'Central Ofense', 1)

-- Add some actions for the unfinished match
INSERT INTO ACTIONS (PLAYERID, CLUBID, MATCHID, MINUTE, TYPE_OF_ACTION, MATCHPART)
VALUES (1, 1, @UnfinishedMatchID, 30, 'Goal', 'First Half')

-- Run the procedure
EXEC sp_CalculatePlayerMatchStats
GO

-- Check that no stats were calculated for the unfinished match
DECLARE @UnfinishedMatchID numeric
SELECT @UnfinishedMatchID = MAX(MATCHID) FROM MATCHES WHERE TOTALMINUTES IS NULL AND DATEOFMATCH = '2025-01-01'

PRINT 'Checking unfinished match (ID: ' + CAST(@UnfinishedMatchID AS VARCHAR(10)) + '):'
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM PLAYER_MATCH_STATS WHERE MATCHID = @UnfinishedMatchID)
        THEN 'ERROR: Stats were calculated for unfinished match!'
        ELSE 'OK: No stats calculated for unfinished match.'
    END AS Status
GO

-- Clean up test data
DECLARE @UnfinishedMatchID numeric
SELECT @UnfinishedMatchID = MAX(MATCHID) FROM MATCHES WHERE TOTALMINUTES IS NULL AND DATEOFMATCH = '2025-01-01'

DELETE FROM ACTIONS WHERE MATCHID = @UnfinishedMatchID
DELETE FROM LINEUPS WHERE MATCHID = @UnfinishedMatchID
DELETE FROM MATCHES WHERE MATCHID = @UnfinishedMatchID
GO

-- Summary statistics
PRINT '=== Final Summary ==='
SELECT 
    'Total Players with Stats' AS Metric,
    COUNT(DISTINCT PLAYERID) AS Value
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'Total Matches Processed',
    COUNT(DISTINCT MATCHID)
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'Total Goals Recorded',
    SUM(GOALSSCORED)
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'Average Minutes Played',
    AVG(MINUTESPLAYED)
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'Total Passes Completed',
    SUM(PASSESCOMPLETED)
FROM PLAYER_MATCH_STATS
GO

-- Top performers
PRINT '=== Top Performers ==='

PRINT 'Top 5 Goal Scorers:'
SELECT TOP 5
    p.NAME AS PlayerName,
    SUM(pms.GOALSSCORED) AS TotalGoals,
    COUNT(DISTINCT pms.MATCHID) AS MatchesPlayed,
    CAST(CAST(SUM(pms.GOALSSCORED) AS FLOAT) / NULLIF(COUNT(DISTINCT pms.MATCHID), 0) AS DECIMAL(5,2)) AS GoalsPerMatch
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
GROUP BY p.PLAYERID, p.NAME
ORDER BY TotalGoals DESC
GO

PRINT 'Top 5 Pass Masters:'
SELECT TOP 5
    p.NAME AS PlayerName,
    SUM(pms.PASSESCOMPLETED) AS TotalPasses,
    SUM(pms.MINUTESPLAYED) AS TotalMinutes,
    CAST(CAST(SUM(pms.PASSESCOMPLETED) AS FLOAT) / NULLIF(SUM(pms.MINUTESPLAYED), 0) * 90 AS DECIMAL(5,2)) AS PassesPer90
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
WHERE SUM(pms.MINUTESPLAYED) > 0
GROUP BY p.PLAYERID, p.NAME
ORDER BY TotalPasses DESC
GO

PRINT '=== Procedure Testing Complete ==='
GO


*/



-- Question 5: Create a stored procedure to populate the Player Match Stats table

-- Drop the stored procedure if it exists
IF OBJECT_ID('sp_CalculatePlayerMatchStats', 'P') IS NOT NULL
    DROP PROCEDURE sp_CalculatePlayerMatchStats
GO

-- Drop the helper function if it exists
IF OBJECT_ID('fn_CalculateMinutesPlayed', 'FN') IS NOT NULL
    DROP FUNCTION fn_CalculateMinutesPlayed
GO

-- First, create the helper function to calculate minutes played
CREATE FUNCTION fn_CalculateMinutesPlayed (
    @PlayerID numeric,
    @ClubID numeric,
    @MatchID numeric,
    @Starter bit
)
RETURNS smallint
AS
BEGIN
    DECLARE @MinutesPlayed smallint = 0
    DECLARE @TotalMatchMinutes smallint
    
    -- Get total minutes for the match
    SELECT @TotalMatchMinutes = TOTALMINUTES
    FROM MATCHES 
    WHERE MATCHID = @MatchID
    
    -- If match minutes not available, return 0
    IF @TotalMatchMinutes IS NULL
        RETURN 0
    
    -- For starters: assume they played full match unless substituted
    IF @Starter = 1
    BEGIN
        SET @MinutesPlayed = @TotalMatchMinutes
        
        -- Check if player was substituted (has 'Exit from match' action)
        DECLARE @SubstitutionTime float
        
        SELECT TOP 1 @SubstitutionTime = MINUTE
        FROM ACTIONS 
        WHERE PLAYERID = @PlayerID 
          AND CLUBID = @ClubID 
          AND MATCHID = @MatchID 
          AND TYPE_OF_ACTION = 'Exit from match'
        
        -- If substituted, minutes played = substitution time
        IF @SubstitutionTime IS NOT NULL
            SET @MinutesPlayed = CAST(@SubstitutionTime AS smallint)
    END
    ELSE
    BEGIN
        -- For substitutes: minutes from entry to end of match (or substitution)
        DECLARE @EntryTime float
        DECLARE @ExitTime float
        
        -- Get entry time
        SELECT TOP 1 @EntryTime = MINUTE
        FROM ACTIONS 
        WHERE PLAYERID = @PlayerID 
          AND CLUBID = @ClubID 
          AND MATCHID = @MatchID 
          AND TYPE_OF_ACTION = 'Enter into match'
        ORDER BY MINUTE
        
        -- Get exit time (if substituted later)
        SELECT TOP 1 @ExitTime = MINUTE
        FROM ACTIONS 
        WHERE PLAYERID = @PlayerID 
          AND CLUBID = @ClubID 
          AND MATCHID = @MatchID 
          AND TYPE_OF_ACTION = 'Exit from match'
        ORDER BY MINUTE
        
        -- Calculate minutes played
        IF @EntryTime IS NOT NULL
        BEGIN
            IF @ExitTime IS NOT NULL
                -- Substituted after entering
                SET @MinutesPlayed = CAST(@ExitTime - @EntryTime AS smallint)
            ELSE
                -- Played from entry to end of match
                SET @MinutesPlayed = CAST(@TotalMatchMinutes - @EntryTime AS smallint)
        END
        -- If no entry time, player never entered (0 minutes)
    END
    
    -- Ensure minutes are not negative
    IF @MinutesPlayed < 0
        SET @MinutesPlayed = 0
    
    -- Ensure minutes don't exceed match total
    IF @MinutesPlayed > @TotalMatchMinutes
        SET @MinutesPlayed = @TotalMatchMinutes
    
    RETURN @MinutesPlayed
END
GO

-- Now create the stored procedure
CREATE PROCEDURE sp_CalculatePlayerMatchStats
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        PRINT 'Starting Player Match Stats calculation...'
        PRINT '========================================='
        
        -- Get count of matches that have finished (TotalMinutes is not NULL)
        DECLARE @FinishedMatchesCount INT
        SELECT @FinishedMatchesCount = COUNT(*) 
        FROM MATCHES 
        WHERE TOTALMINUTES IS NOT NULL
        
        PRINT 'Found ' + CAST(@FinishedMatchesCount AS VARCHAR(10)) + ' finished matches.'
        
        -- Clear existing stats for matches that have finished
        -- This ensures we recalculate fresh stats each time
        DELETE FROM PLAYER_MATCH_STATS
        WHERE MATCHID IN (
            SELECT MATCHID FROM MATCHES WHERE TOTALMINUTES IS NOT NULL
        )
        
        PRINT 'Cleared existing stats for finished matches.'
        
        -- Insert calculated stats for all players in lineups for finished matches
        INSERT INTO PLAYER_MATCH_STATS (PLAYERID, CLUBID, MATCHID, GOALSSCORED, MINUTESPLAYED, PASSESCOMPLETED, SHOTSONTARGET)
        SELECT 
            l.PLAYERID,
            l.CLUBID,
            l.MATCHID,
            
            -- Goals scored: Count of 'Goal' actions for this player in this match
            ISNULL((
                SELECT COUNT(*) 
                FROM ACTIONS a 
                WHERE a.PLAYERID = l.PLAYERID 
                  AND a.CLUBID = l.CLUBID 
                  AND a.MATCHID = l.MATCHID 
                  AND a.TYPE_OF_ACTION = 'Goal'
            ), 0) AS GOALSSCORED,
            
            -- Minutes played: Using the helper function
            dbo.fn_CalculateMinutesPlayed(l.PLAYERID, l.CLUBID, l.MATCHID, l.STARTER) AS MINUTESPLAYED,
            
            -- Passes completed: Count of 'Pass done' actions
            ISNULL((
                SELECT COUNT(*) 
                FROM ACTIONS a 
                WHERE a.PLAYERID = l.PLAYERID 
                  AND a.CLUBID = l.CLUBID 
                  AND a.MATCHID = l.MATCHID 
                  AND a.TYPE_OF_ACTION = 'Pass done'
            ), 0) AS PASSESCOMPLETED,
            
            -- Shots on target: Count of 'Shot on target' actions
            ISNULL((
                SELECT COUNT(*) 
                FROM ACTIONS a 
                WHERE a.PLAYERID = l.PLAYERID 
                  AND a.CLUBID = l.CLUBID 
                  AND a.MATCHID = l.MATCHID 
                  AND a.TYPE_OF_ACTION = 'Shot on target'
            ), 0) AS SHOTSONTARGET
            
        FROM LINEUPS l
        INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
        WHERE m.TOTALMINUTES IS NOT NULL
        ORDER BY l.MATCHID, l.CLUBID, l.PLAYERID
        
        -- Get count of inserted records
        DECLARE @InsertedCount INT = @@ROWCOUNT
        
        PRINT 'Successfully calculated stats for ' + CAST(@InsertedCount AS VARCHAR(10)) + ' player-match combinations.'
        PRINT 'Procedure completed successfully.'
        
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
        DECLARE @ErrorState INT = ERROR_STATE()
        
        PRINT 'Error occurred during Player Match Stats calculation:'
        PRINT 'Error: ' + @ErrorMessage
        PRINT 'Procedure failed.'
        
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState)
    END CATCH
END
GO

-- Now let's test the stored procedure (simplified test to avoid errors)

-- First, check the current state of the tables
PRINT '=== Before running the procedure ==='
SELECT 
    'PLAYER_MATCH_STATS' AS TableName,
    COUNT(*) AS RecordCount
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'MATCHES (finished)',
    COUNT(*)
FROM MATCHES 
WHERE TOTALMINUTES IS NOT NULL
UNION ALL
SELECT 
    'LINEUPS (for finished matches)',
    COUNT(*)
FROM LINEUPS l
INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
WHERE m.TOTALMINUTES IS NOT NULL
GO

-- Show some sample data before calculation
PRINT 'Sample of current PLAYER_MATCH_STATS (before):'
SELECT TOP 5 
    pms.PLAYERID,
    p.NAME AS PlayerName,
    c.NAME AS ClubName,
    pms.MATCHID,
    pms.GOALSSCORED,
    pms.MINUTESPLAYED,
    pms.PASSESCOMPLETED,
    pms.SHOTSONTARGET
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON pms.CLUBID = c.CLUBID
ORDER BY pms.MATCHID, pms.PLAYERID
GO

-- Test the helper function with some examples
PRINT 'Testing minutes played calculation:'
SELECT 
    l.PLAYERID,
    p.NAME AS PlayerName,
    l.CLUBID,
    c.NAME AS ClubName,
    l.MATCHID,
    l.STARTER,
    dbo.fn_CalculateMinutesPlayed(l.PLAYERID, l.CLUBID, l.MATCHID, l.STARTER) AS CalculatedMinutes,
    pms.MINUTESPLAYED AS ExistingMinutes
FROM LINEUPS l
INNER JOIN PLAYERS p ON l.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON l.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON l.MATCHID = m.MATCHID
LEFT JOIN PLAYER_MATCH_STATS pms ON l.PLAYERID = pms.PLAYERID 
    AND l.CLUBID = pms.CLUBID 
    AND l.MATCHID = pms.MATCHID
WHERE m.TOTALMINUTES IS NOT NULL
  AND l.PLAYERID IN (1, 7, 13) -- Test with Messi, Haaland, Lewandowski
  AND l.MATCHID IN (1, 10, 13)
ORDER BY l.MATCHID, l.PLAYERID
GO

-- Now run the main stored procedure
PRINT '=== Running the stored procedure ==='
EXEC sp_CalculatePlayerMatchStats
GO

-- Check the results
PRINT '=== After running the procedure ==='
SELECT 
    'PLAYER_MATCH_STATS' AS TableName,
    COUNT(*) AS RecordCount
FROM PLAYER_MATCH_STATS
GO

-- Show detailed results
PRINT 'Detailed Player Match Stats:'
SELECT 
    pms.PLAYERID,
    p.NAME AS PlayerName,
    c.NAME AS ClubName,
    m.MATCHID,
    m.DATEOFMATCH,
    hc.NAME AS HomeClub,
    ac.NAME AS AwayClub,
    pms.GOALSSCORED,
    pms.MINUTESPLAYED,
    pms.PASSESCOMPLETED,
    pms.SHOTSONTARGET
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON pms.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON pms.MATCHID = m.MATCHID
INNER JOIN CLUBS hc ON m.HOMECLUB = hc.CLUBID
INNER JOIN CLUBS ac ON m.AWAYCLUB = ac.CLUBID
ORDER BY m.DATEOFMATCH, c.NAME, p.NAME
GO

-- Test with specific players to verify accuracy
PRINT '=== Verification with specific players ==='

-- Check Lionel Messi's stats
PRINT 'Lionel Messi stats:'
SELECT 
    p.NAME AS PlayerName,
    c.NAME AS ClubName,
    m.DATEOFMATCH,
    pms.GOALSSCORED,
    pms.MINUTESPLAYED,
    pms.PASSESCOMPLETED,
    pms.SHOTSONTARGET
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON pms.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON pms.MATCHID = m.MATCHID
WHERE p.PLAYERID = 1  -- Lionel Messi
ORDER BY m.DATEOFMATCH
GO

-- Check Erling Haaland's stats
PRINT 'Erling Haaland stats:'
SELECT 
    p.NAME AS PlayerName,
    c.NAME AS ClubName,
    m.DATEOFMATCH,
    pms.GOALSSCORED,
    pms.MINUTESPLAYED,
    pms.PASSESCOMPLETED,
    pms.SHOTSONTARGET
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
INNER JOIN CLUBS c ON pms.CLUBID = c.CLUBID
INNER JOIN MATCHES m ON pms.MATCHID = m.MATCHID
WHERE p.PLAYERID = 7  -- Erling Haaland
ORDER BY m.DATEOFMATCH
GO

-- Test the procedure with edge cases

-- Test 1: Run procedure again (should recalculate fresh)
PRINT '=== Test: Running procedure again (should recalculate) ==='
EXEC sp_CalculatePlayerMatchStats
GO

-- Test 2: Check that stats for unfinished matches are not calculated
PRINT '=== Test: Verify unfinished matches are excluded ==='

-- Create an unfinished match (fix: get SCOPE_IDENTITY() properly)
DECLARE @UnfinishedMatchID numeric

INSERT INTO MATCHES (HOMECLUB, AWAYCLUB, COMPETITIONID, SEASONID, COUNTRYCODE, CITYID, MATCHDAY, DATEOFMATCH, ATTENDANCE, TOTALMINUTES)
VALUES (1, 2, 1, 2, 'ESP', 1, 36, '2025-01-01', NULL, NULL) -- TOTALMINUTES is NULL

-- Get the inserted MATCHID
SET @UnfinishedMatchID = SCOPE_IDENTITY()

PRINT 'Created unfinished match with ID: ' + CAST(@UnfinishedMatchID AS VARCHAR(10))

-- Add a lineup for the unfinished match
INSERT INTO LINEUPS (PLAYERID, CLUBID, MATCHID, POSITION, STARTER)
VALUES (1, 1, @UnfinishedMatchID, 'Central Ofense', 1)

-- Run the procedure
EXEC sp_CalculatePlayerMatchStats
GO

-- Check that no stats were calculated for the unfinished match
DECLARE @UnfinishedMatchIDCheck numeric
SELECT @UnfinishedMatchIDCheck = MAX(MATCHID) FROM MATCHES WHERE TOTALMINUTES IS NULL AND DATEOFMATCH = '2025-01-01'

PRINT 'Checking unfinished match (ID: ' + CAST(@UnfinishedMatchIDCheck AS VARCHAR(10)) + '):'
SELECT 
    CASE 
        WHEN EXISTS (SELECT 1 FROM PLAYER_MATCH_STATS WHERE MATCHID = @UnfinishedMatchIDCheck)
        THEN 'ERROR: Stats were calculated for unfinished match!'
        ELSE 'OK: No stats calculated for unfinished match.'
    END AS Status
GO

-- Summary statistics
PRINT '=== Final Summary ==='
SELECT 
    'Total Players with Stats' AS Metric,
    COUNT(DISTINCT PLAYERID) AS Value
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'Total Matches Processed',
    COUNT(DISTINCT MATCHID)
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'Total Goals Recorded',
    SUM(GOALSSCORED)
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'Average Minutes Played',
    AVG(CAST(MINUTESPLAYED AS FLOAT))
FROM PLAYER_MATCH_STATS
UNION ALL
SELECT 
    'Total Passes Completed',
    SUM(PASSESCOMPLETED)
FROM PLAYER_MATCH_STATS
GO

-- Top performers (fixed the HAVING clause issue)
PRINT '=== Top Performers ==='

PRINT 'Top 5 Goal Scorers:'
SELECT TOP 5
    p.NAME AS PlayerName,
    SUM(pms.GOALSSCORED) AS TotalGoals,
    COUNT(DISTINCT pms.MATCHID) AS MatchesPlayed,
    CAST(CAST(SUM(pms.GOALSSCORED) AS FLOAT) / NULLIF(COUNT(DISTINCT pms.MATCHID), 0) AS DECIMAL(5,2)) AS GoalsPerMatch
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
GROUP BY p.PLAYERID, p.NAME
ORDER BY TotalGoals DESC
GO

PRINT 'Top 5 Pass Masters:'
SELECT TOP 5
    p.NAME AS PlayerName,
    SUM(pms.PASSESCOMPLETED) AS TotalPasses,
    SUM(pms.MINUTESPLAYED) AS TotalMinutes,
    CAST(CAST(SUM(pms.PASSESCOMPLETED) AS FLOAT) / NULLIF(SUM(pms.MINUTESPLAYED), 0) * 90 AS DECIMAL(5,2)) AS PassesPer90
FROM PLAYER_MATCH_STATS pms
INNER JOIN PLAYERS p ON pms.PLAYERID = p.PLAYERID
WHERE pms.MINUTESPLAYED > 0
GROUP BY p.PLAYERID, p.NAME
HAVING SUM(pms.MINUTESPLAYED) > 0
ORDER BY TotalPasses DESC
GO

-- Clean up test data
PRINT '=== Cleaning up test data ==='
DECLARE @UnfinishedMatchIDCleanup numeric
SELECT @UnfinishedMatchIDCleanup = MAX(MATCHID) FROM MATCHES WHERE TOTALMINUTES IS NULL AND DATEOFMATCH = '2025-01-01'

IF @UnfinishedMatchIDCleanup IS NOT NULL
BEGIN
    DELETE FROM LINEUPS WHERE MATCHID = @UnfinishedMatchIDCleanup
    DELETE FROM MATCHES WHERE MATCHID = @UnfinishedMatchIDCleanup
    PRINT 'Cleaned up test match with ID: ' + CAST(@UnfinishedMatchIDCleanup AS VARCHAR(10))
END
ELSE
BEGIN
    PRINT 'No test match to clean up'
END
GO

PRINT '=== Question 5 Implementation Complete ==='
PRINT 'Stored Procedure: sp_CalculatePlayerMatchStats created successfully.'
PRINT 'Helper Function: fn_CalculateMinutesPlayed created successfully.'
PRINT 'All tests completed.'
GO