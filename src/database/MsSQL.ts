import { IDatabase } from './IDatabase';
import { Game, GameOptions, Score } from '../Game';
import { GameId } from '../common/Types';
import { SerializedGame } from '../SerializedGame';
import { ConnectionPool, config } from 'mssql';

export class MsSQL implements IDatabase {
    private client: ConnectionPool;
    private databaseName: string | undefined = 'terraforming'; // Use this only for stats.

    protected statistics = {
        saveCount: 0,
        saveErrorCount: 0,
        saveConflictUndoCount: 0,
        saveConflictNormalCount: 0,
    };

    constructor() {
        const config: config = {
            user: process.env.MSSQL_USER,
            password: process.env.MSSQL_PASSWORD,
            database: process.env.MSSQL_DATABASE,
            server: process.env.MSSQL_SERVER ?? 'localhost',
            pool: {
                max: 10,
                min: 0,
                idleTimeoutMillis: 30000
            },
            options: {
                trustServerCertificate: true
            }
        };

        this.client = new ConnectionPool(config, (err) => {
            if (err) {
                throw err;
            };
        });

        
        this.client.connect().then(() => {
            this.client.query(`
                IF OBJECT_ID(\'dbo.games\', \'U\') IS NULL
                BEGIN
                    CREATE TABLE dbo.games (
                        game_id nvarchar(450),
                        players int,
                        save_id int,
                        game nvarchar(max),
                        status nvarchar(max) DEFAULT \'running\',
                        created_time datetime DEFAULT GETDATE(),
                        PRIMARY KEY (game_id, save_id))
                END`,
                (err) => {
                    if (err) {
                        throw err;
                    }
                });

            this.client.query(`
                IF OBJECT_ID(\'dbo.game_results\', \'U\') IS NULL
                BEGIN
                    CREATE TABLE dbo.game_results (
                        game_id nvarchar(450) NOT NULL,
                        seed_game_id nvarchar(max),
                        players int,
                        generations int,
                        game_options nvarchar(max),
                        scores nvarchar(max),
                        PRIMARY KEY (game_id))
                END`,
                (err) => {
                    if (err) {
                        throw err;
                    }
                });

            this.client.query(`
                IF NOT EXISTS(
                    SELECT 1
                    FROM sys.indexes
                    WHERE name = \'IX_games_save_id\' AND object_id = OBJECT_ID(\'dbo.games\'))
                BEGIN
                    CREATE NONCLUSTERED INDEX IX_games_save_id
                    ON dbo.games(save_id)
                END`,
                (err) => {
                    if (err) {
                        throw err;
                    }
                });

            this.client.query(`
                IF NOT EXISTS(
                    SELECT 1
                    FROM sys.indexes
                    WHERE name = \'IX_games_created_time\' AND object_id = OBJECT_ID(\'dbo.games\'))
                BEGIN
                    CREATE NONCLUSTERED INDEX IX_games_created_time
                    ON dbo.games(created_time)
                END`,
                (err) => {
                    if (err) {
                        throw err;
                    }
                });
        });
    }

    async initialize(): Promise<void> {

    }

    public async getPlayerCount(game_id: GameId): Promise<number> {
        const sql = 'SELECT TOP 1 players FROM games WHERE save_id = 0 AND game_id = @game_id';

        const res = await this.client
            .request()
            .input('game_id', game_id)
            .query<any>(sql);

        if (res?.recordsets[0].length === 0) {
            throw new Error(`no rows found for game id ${game_id}`);
        }
        return res?.recordsets[0][0].players;
    }

    public async getGameIds(): Promise<Array<GameId>> {
        // To only load incomplete games add `WHERE status=\'running\'`
        // above "GROUP BY game_id) a"
        const sql: string = `
            SELECT games.game_id
            FROM games, (
              SELECT max(save_id) save_id, game_id
              FROM games
              GROUP BY game_id) a
            WHERE games.game_id = a.game_id
            AND games.save_id = a.save_id
            ORDER BY created_time DESC`;
        const res = await this.client
            .request()
            .query<any>(sql);

        return res?.recordsets[0].map((row) => row.game_id);
    }

    public loadCloneableGame(game_id: GameId): Promise<SerializedGame> {
        return this.getGameVersion(game_id, 0);
    }

    public async getGame(game_id: GameId): Promise<SerializedGame> {
        // Retrieve last save from database
        const res = await this.client
            .request()
            .input('game_id', game_id)
            .query<any>('SELECT TOP 1 game game FROM games WHERE game_id = @game_id ORDER BY save_id DESC');
        const json = JSON.parse(res?.recordsets[0][0].game);
        return json;
    }
    
    public async getGameId(id: string): Promise<GameId> {
        let sql = undefined;
        if (id.charAt(0) === 'p') {
            sql =
                `   SELECT game_id
                    FROM games
                    OUTER APPLY OPENJSON(game) WITH ( players NVARCHAR(MAX) '$.players' AS JSON) AS j1
                    OUTER APPLY OPENJSON(j1.players) WITH (player NVARCHAR(max) '$.id') AS j2
                    WHERE save_id = 0 AND j2.player = @id`;
        } else if (id.charAt(0) === 's') {
            sql =
                `   SELECT game_id
                    FROM games
                    OUTER APPLY OPENJSON(game) WITH (spectator NVARCHAR(MAX) '$.spectatorId') AS j1
                    WHERE save_id = 0 AND j1.spectator = @id`;
        } else {
            throw new Error(`id ${id} is neither a player id nor spectator id`);
        }

        try {
            const res = await this.client
                .request()
                .input('id', id)
                .query<any>(sql);
            if (res?.recordsets[0].length === 0) {
                throw new Error(`Game for player id ${id} not found`);
            }
            return res?.recordsets[0][0].game_id;
        } catch (err) {
            console.error('MsSQL:getGameId', err);
            throw err;
        }
    }

    public async getSaveIds(gameId: GameId): Promise<Array<number>> {
        const res = await this.client
            .request()
            .input('game_id', gameId)
            .query<any>('SELECT distinct save_id FROM games WHERE game_id = @game_id')

        const allSaveIds: Array<number> = [];
        res?.recordsets[0].forEach((row) => {
            allSaveIds.push(row.save_id);
        });
        return Promise.resolve(allSaveIds);
    }

    async getGameVersion(game_id: GameId, save_id: number): Promise<SerializedGame> {
        const res = await this.client
            .request()
            .input('game_id', game_id)
            .input('save_id', save_id)
            .query<any>('SELECT game game FROM games WHERE game_id = @game_id and save_id = @save_id');

        if (res?.recordsets[0].length === 0) {
            throw new Error(`Game ${game_id} not found at save_id ${save_id}`);
        }

        return JSON.parse(res?.recordsets[0][0].game);
    }

    saveGameResults(game_id: GameId, players: number, generations: number, gameOptions: GameOptions, scores: Array<Score>): void {
        this.client
            .request()
            .input('game_id', game_id)
            .input('seed_game_id', gameOptions.clonedGamedId)
            .input('players', players)
            .input('generations', generations)
            .input('game_options', JSON.stringify(gameOptions))
            .input('scores', JSON.stringify(scores))
            .query<any>('INSERT INTO game_results (game_id, seed_game_id, players, generations, game_options, scores) VALUES(@game_id, @seed_game_id, @players, @generations, @game_options, @scores)', (err) => {
                if (err) {
                    console.error('MsSQL:saveGameResults', err);
                    throw err;
                }
            });
    }

    async getMaxSaveId(game_id: GameId): Promise<SerializedGame> {
        const res = await this.client
            .request()
            .input('game_id', game_id)
            .query<any>('SELECT MAX(save_id) as save_id FROM games WHERE game_id = @game_id');
        return res?.recordsets[0][0].save_id;
    }

    throwIf(err: any, condition: string) {
        if (err) {
            console.error('MsSQL', condition, err);
            throw err;
        }
    }

    async cleanGame(game_id: GameId): Promise<void> {
        const maxSaveId = await this.getMaxSaveId(game_id);
        // DELETE all saves except initial and last one
        const delete1 = this.client
            .request()
            .input('game_id', game_id)
            .input('save_id', maxSaveId)
            .query<any>('DELETE FROM games WHERE game_id = @game_id AND save_id < @save_id AND save_id > 0');
        // Flag game as finished
        const delete2 = this.client
            .request()
            .input('game_id', game_id)
            .query('UPDATE games SET status = \'finished\' WHERE game_id = @game_id');
        // Purge after setting the status as finished so it does not delete the game.
        const delete3 = this.purgeUnfinishedGames();
        await Promise.all([delete1, delete2, delete3]);
    }

    // Purge unfinished games older than MAX_GAME_DAYS days. If this environment variable is absent, it uses the default of 10 days.
    async purgeUnfinishedGames(maxGameDays: string | undefined = process.env.MAX_GAME_DAYS): Promise<void> {
        const envDays = parseInt(maxGameDays || '');
        const days = Number.isInteger(envDays) ? envDays : 9999;
        await this.client
            .request()
            .input('days', days)
            .query<any>('DELETE FROM games WHERE created_time < DATEADD(DAY, -1 * @days, GETDATE())', function (err: Error | undefined, res) {
                if (res) {
                    console.log(`Purged ${res?.rowsAffected[0]} rows`);
                }
                if (err) {
                    return console.warn(err.message);
                }
            });
    }

    async restoreGame(game_id: GameId, save_id: number): Promise<SerializedGame> {
        // Retrieve last save from database
        logForUndo(game_id, 'restore to', save_id);
        const res = await this.client
            .request()
            .input('game_id', game_id)
            .input('save_id', save_id)
            .query<any>('SELECT TOP 1 game game FROM games WHERE game_id = @game_id AND save_id = @save_id ORDER BY save_id DESC');

        if (res?.recordsets[0].length === 0) {
            throw new Error(`Game ${game_id} not found`);
        }
        try {
            // Transform string to json
            const json = JSON.parse(res?.recordsets[0][0].game);
            logForUndo(json.id, 'restored to', json.lastSaveId, 'from', save_id);
            return json;
        } catch (e) {
            const error = e instanceof Error ? e : new Error(String(e));
            throw error;
        }
    }

    saveGame(game: Game): Promise<void> {
        const gameJSON = game.toJSON();
        this.statistics.saveCount++;
        if (game.gameOptions.undoOption) logForUndo(game.id, 'start save', game.lastSaveId);
        return this.client
            .request()
            .input('game_id', game.id)
            .input('save_id', game.lastSaveId)
            .input('game', gameJSON)
            .input('players', game.getPlayers().length)
            .query<any>(`
                    MERGE games AS g
                    USING (
                      SELECT
                        @game_id AS game_id,
                        @save_id AS save_id,
                        @game AS game,
                        @players AS players
                     ) AS source
                    ON g.game_id = source.game_id AND g.save_id = source.save_id
                    WHEN NOT MATCHED THEN
                      INSERT(game_id, save_id, game, players)
                      VALUES(source.game_id, source.save_id, source.game, source.players)
                    WHEN MATCHED THEN
                      UPDATE SET
                        g.game = source.game
                    OUTPUT inserted.*;`)
            .then((res) => {
                let inserted: boolean = true;
                try {
                    inserted = res?.recordsets[0].length > 0;
                } catch (err) {
                    console.error(err);
                }
                if (inserted === false) {
                    if (game.gameOptions.undoOption) {
                        this.statistics.saveConflictUndoCount++;
                    } else {
                        this.statistics.saveConflictNormalCount++;
                    }
                }

                game.lastSaveId++;
                if (game.gameOptions.undoOption) logForUndo(game.id, 'increment save id, now', game.lastSaveId);
            })
            .catch((err) => {
                if (err) {
                    console.error('MsSQL:saveGame', err);
                    return;
                }
            });

    }

    deleteGameNbrSaves(game_id: GameId, rollbackCount: number): void {
        if (rollbackCount <= 0) {
            console.error(`invalid rollback count for ${game_id}: $rollbackCount`);
            return;
        }
        logForUndo(game_id, 'deleting', rollbackCount, 'saves');

        this
            .getSaveIds(game_id)
            .then((first) => {
                this.client
                    .request()
                    .input('game_id', game_id)
                    .input('rollbackCount', rollbackCount)
                    .query<any>(`
                        ;WITH CTE AS
                        (
                          SELECT TOP (@rollbackCount) *
                          FROM games
                          WHERE game_id = @game_id
                          ORDER BY save_id DESC
                        )
                        DELETE FROM CTE`,
                        (err, res) => {
                            if (err) {
                                console.error(err.message);
                            }
                            logForUndo(game_id, 'deleted', res?.rowsAffected, 'rows');
                            this.getSaveIds(game_id)
                                .then((second) => {
                                    const difference = first.filter((x) => !second.includes(x));
                                    logForUndo(game_id, 'second', second);
                                    logForUndo(game_id, 'Rollback difference', difference);
                                });
                        });
            });
    }

    public async stats(): Promise<{ [key: string]: string | number }> {
        const map: { [key: string]: string | number } = {
            'type': 'MSSQL',
            'pool-total-count': this.client.pool?.numUsed(),
            'pool-idle-count': this.client.pool?.numFree(),
            'pool-waiting-count': this.client.pool?.numPendingAcquires(),
            'save-count': this.statistics.saveCount,
            'save-error-count': this.statistics.saveErrorCount,
            'save-confict-normal-count': this.statistics.saveConflictNormalCount,
            'save-confict-undo-count': this.statistics.saveConflictUndoCount,
        };

        // TODO(kberg): return row counts
        const res = await this.client
            .request()
            .input('db_name', this.databaseName)
            .query<any>(
                `
                    SELECT
                      s.Name AS [schema_name],
                      t.Name AS [table_name],
                      p.rows AS [row_counts],
                      1024 * 1024 * CAST(ROUND((SUM(a.used_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS [used_bytes],
                      1024 * 1024 * CAST(ROUND((SUM(a.total_pages) - SUM(a.used_pages)) / 128.00, 2) AS NUMERIC(36, 2)) AS [unused_bytes],
                      1024 * 1024 * CAST(ROUND((SUM(a.total_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS [total_bytes]
                    FROM sys.tables t
                      INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
                      INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
                      INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    GROUP BY t.Name, s.Name, p.Rows
                    ORDER BY s.Name, t.Name;

                    SELECT
                      DB_NAME(database_id) AS [database_name],
                      1024 * SUM(CASE WHEN type_desc = 'LOG' THEN size END) * 8 AS [log_size_bytes],
                      1024 * SUM(CASE WHEN type_desc = 'ROWS' THEN size END) * 8 AS [row_size_bytes],
                      1024 * SUM(size) * 8 AS [total_size_bytes]
                    FROM sys.master_files WITH(NOWAIT)
                    WHERE database_id = DB_ID('terraforming') -- for current db
                    GROUP BY database_id
                `);
        map['size-bytes-games'] = res?.recordsets[0][0].used_bytes;
        map['size-bytes-game-results'] = res?.recordsets[0][1].used_bytes;
        map['size-bytes-database'] = res?.recordsets[1][0].total_size_bytes;
        return map;
    }
}

function logForUndo(gameId: string, ...message: any[]) {
    console.error(['TRACKING:', gameId, ...message]);
}
