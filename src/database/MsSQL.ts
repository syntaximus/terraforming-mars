import { DbLoadCallback, IDatabase } from './IDatabase';
import { Game, GameOptions, Score } from '../Game';
import { GameId } from '../common/Types';
import { IGameData } from './IDatabase';
import { SerializedGame } from '../SerializedGame';
import { ConnectionPool, config } from 'mssql';

export class MsSQL implements IDatabase {
    private client: ConnectionPool;

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

    getClonableGames(cb: (err: Error | undefined, allGames: Array<IGameData>) => void) {
        const allGames: Array<IGameData> = [];
        const sql = 'SELECT distinct game_id game_id, players players FROM games WHERE save_id = 0 order by game_id asc';

        this.client.query<any>(sql, (err, res) => {
            if (err) {
                console.error('MsSQL:getClonableGames', err);
                cb(err, []);
                return;
            }
            for (const row of res?.recordsets[0] ?? []) {

                const gameId: GameId = row?.game_id;
                const playerCount: number = row.players;
                const gameData: IGameData = {
                    gameId,
                    playerCount,
                };
                allGames.push(gameData);
            }
            cb(undefined, allGames);
        });
    }

    getGames(cb: (err: Error | undefined, allGames: Array<GameId>) => void) {
        const allGames: Array<GameId> = [];
        const sql: string = 'SELECT games.game_id FROM games, (SELECT max(save_id) save_id, game_id FROM games WHERE status=\'running\' GROUP BY game_id) a WHERE games.game_id = a.game_id AND games.save_id = a.save_id ORDER BY created_time DESC';
        this.client.query<any>(sql, (err, res) => {
            if (err) {
                console.error('MsSQL:getGames', err);
                cb(err, []);
                return;
            }
            for (const row of res?.recordsets[0] ?? []) {
                allGames.push(row.game_id);
            }
            cb(undefined, allGames);
        });
    }

    loadCloneableGame(game_id: GameId, cb: DbLoadCallback<SerializedGame>) {
        // Retrieve first save from database
        this.client
            .request()
            .input('game_id', game_id)
            .query<any>('SELECT game_id game_id, game game FROM games WHERE game_id = @game_id AND save_id = 0', (err: Error | undefined, res) => {
                if (err) {
                    console.error('MsSQL:restoreReferenceGame', err);
                    return cb(err, undefined);
                }
                if (res?.recordsets[0].length === 0) {
                    return cb(new Error(`Game ${game_id} not found`), undefined);
                }
                try {
                    const json = JSON.parse(res?.recordsets[0][0].game);
                    return cb(undefined, json);
                } catch (exception) {
                    const error = exception instanceof Error ? exception : new Error(String(exception));
                    console.error(`Unable to restore game ${game_id}`, error);
                    cb(error, undefined);
                    return;
                }
            });
    }

    getGame(game_id: GameId, cb: (err: Error | undefined, game?: SerializedGame) => void): void {
        // Retrieve last save from database
        this.client
            .request()
            .input('game_id', game_id)
            .query<any>('SELECT TOP 1 game game FROM games WHERE game_id = @game_id ORDER BY save_id DESC', (err, res) => {
                if (err) {
                    console.error('MsSQL:getGame', err);
                    return cb(err);
                }
                if (res?.recordsets[0].length === 0) {
                    return cb(new Error('Game not found'));
                }
                cb(undefined, JSON.parse(res?.recordsets[0][0].game));
            });
    }

    // TODO(kberg): throw an error if two game ids exist.
    getGameId(_playerId: string, _cb: (err: Error | undefined, gameId?: GameId) => void): void {
        throw new Error('Not implemented');
    }

    getGameVersion(game_id: GameId, save_id: number, cb: DbLoadCallback<SerializedGame>): void {
        this.client
            .request()
            .input('game_id', game_id)
            .input('save_id', save_id)
            .query<any>('SELECT game game FROM games WHERE game_id = @game_id and save_id = @save_id', (err: Error | undefined, res) => {
                if (err) {
                    console.error('MsSQL:getGameVersion', err);
                    return cb(err, undefined);
                }
                cb(undefined, JSON.parse(res?.recordsets[0][0].game));
            });
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

    getMaxSaveId(game_id: GameId, cb: DbLoadCallback<number>): void {
        this.client
            .request()
            .input('game_id', game_id)
            .query<any>('SELECT MAX(save_id) as save_id FROM games WHERE game_id = @game_id', (err: Error | undefined, res) => {
                if (err) {
                    return cb(err ?? undefined, undefined);
                }
                cb(undefined, res?.recordsets[0][0].save_id);
            });
    }

    throwIf(err: any, condition: string) {
        if (err) {
            console.error('MsSQL', condition, err);
            throw err;
        }
    }

    cleanSaves(game_id: GameId): void {
        this.getMaxSaveId(game_id, ((err, save_id) => {
            this.throwIf(err, 'cleanSaves0');
            if (save_id === undefined) throw new Error('saveId is undefined for ' + game_id);
            // DELETE all saves except initial and last one
            this.client
                .request()
                .input('game_id', game_id)
                .input('save_id', save_id)
                .query<any>('DELETE FROM games WHERE game_id = @game_id AND save_id < @save_id AND save_id > 0', (err) => {
                    this.throwIf(err, 'cleanSaves1');
                    // Flag game as finished
                    this.client
                        .request()
                        .input('game_id', game_id)
                        .query('UPDATE games SET status = \'finished\' WHERE game_id = @game_id', (err2) => {
                            this.throwIf(err2, 'cleanSaves2');
                            // Purge after setting the status as finished so it does not delete the game.
                            this.purgeUnfinishedGames();
                        });
                });
        }));
    }

    // Purge unfinished games older than MAX_GAME_DAYS days. If this environment variable is absent, it uses the default of 10 days.
    purgeUnfinishedGames(): void {
        const envDays = parseInt(process.env.MAX_GAME_DAYS || '');
        const days = Number.isInteger(envDays) ? envDays : 10;
        this.client
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

    restoreGame(game_id: GameId, save_id: number, cb: DbLoadCallback<Game>): void {
        // Retrieve last save from database
        this.client
            .request()
            .input('game_id', game_id)
            .input('save_id', save_id)
            .query<any>('SELECT TOP 1 game game FROM games WHERE game_id = @game_id AND save_id = @save_id ORDER BY save_id DESC', (err, res) => {
                if (err) {
                    console.error('MsSQL:restoreGame', err);
                    cb(err, undefined);
                    return;
                }
                if (res?.recordsets[0].length === 0) {
                    console.error('MsSQL:restoreGame', `Game ${game_id} not found`);
                    cb(err, undefined);
                    return;
                }
                try {
                    // Transform string to json
                    const json = JSON.parse(res?.recordsets[0][0].game);
                    const game = Game.deserialize(json);
                    cb(undefined, game);
                } catch (e) {
                    const error = e instanceof Error ? e : new Error(String(e));
                    cb(error, undefined);
                }
            });
    }

    saveGame(game: Game): Promise<void> {
        const gameJSON = game.toJSON();
        this.client
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
                        g.game = source.game;`,
                (err) => {
                    if (err) {
                        console.error('MsSQL:saveGame', err);
                        return;
                    }
                },
            );

        // This must occur after the save.
        game.lastSaveId++;
        return Promise.resolve();
    }

    deleteGameNbrSaves(game_id: GameId, rollbackCount: number): void {
        if (rollbackCount > 0) {
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
                    (err) => {
                        if (err) {
                            return console.warn(err.message);
                        }
                    });
        }
    }
}
