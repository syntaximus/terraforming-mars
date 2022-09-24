import * as http from 'http';

import {ApiCloneableGame} from './routes/ApiCloneableGame';
import {ApiGameLogs} from './routes/ApiGameLogs';
import {ApiGames} from './routes/ApiGames';
import {ApiGame} from './routes/ApiGame';
import {ApiGameHistory} from './routes/ApiGameHistory';
import {ApiPlayer} from './routes/ApiPlayer';
import {ApiStats} from './routes/ApiStats';
import {ApiMetrics} from './routes/ApiMetrics';
import {ApiSpectator} from './routes/ApiSpectator';
import {ApiWaitingFor} from './routes/ApiWaitingFor';
import {GameHandler} from './routes/Game';
import {GameLoader} from './database/GameLoader';
import {GamesOverview} from './routes/GamesOverview';
import {IHandler} from './routes/IHandler';
import {Load} from './routes/Load';
import {LoadGame} from './routes/LoadGame';
import {Route} from './routes/Route';
import {PlayerInput} from './routes/PlayerInput';
import {ServeApp} from './routes/ServeApp';
import {ServeAsset} from './routes/ServeAsset';

const handlers: Map<string, IHandler> = new Map(
  [
    ['/terraforming/', ServeApp.INSTANCE],
    ['/terraforming/admin', ServeApp.INSTANCE],
    ['/terraforming/api/cloneablegame', ApiCloneableGame.INSTANCE],
    ['/terraforming/api/game', ApiGame.INSTANCE],
    ['/terraforming/api/game/history', ApiGameHistory.INSTANCE],
    ['/terraforming/api/game/logs', ApiGameLogs.INSTANCE],
    ['/terraforming/api/games', ApiGames.INSTANCE],
    ['/terraforming/api/metrics', ApiMetrics.INSTANCE],
    ['/terraforming/api/player', ApiPlayer.INSTANCE],
    ['/terraforming/api/stats', ApiStats.INSTANCE],
    ['/terraforming/api/spectator', ApiSpectator.INSTANCE],
    ['/terraforming/api/waitingfor', ApiWaitingFor.INSTANCE],
    ['/terraforming/cards', ServeApp.INSTANCE],
    ['/terraforming/favicon.ico', ServeAsset.INSTANCE],
    ['/terraforming/game', GameHandler.INSTANCE],
    ['/terraforming/games-overview', GamesOverview.INSTANCE],
    ['/terraforming/help', ServeApp.INSTANCE],
    ['/terraforming/load', Load.INSTANCE],
    ['/terraforming/load_game', LoadGame.INSTANCE],
    ['/terraforming/main.js', ServeAsset.INSTANCE],
    ['/terraforming/main.js.map', ServeAsset.INSTANCE],
    ['/terraforming/new-game', ServeApp.INSTANCE],
    ['/terraforming/player', ServeApp.INSTANCE],
    ['/terraforming/player/input', PlayerInput.INSTANCE],
    ['/terraforming/spectator', ServeApp.INSTANCE],
    ['/terraforming/styles.css', ServeAsset.INSTANCE],
    ['/terraforming/sw.js', ServeAsset.INSTANCE],
    ['/terraforming/the-end', ServeApp.INSTANCE],
  ],
);

export function processRequest(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  route: Route,
  serverId: string): void {
  if (req.method === 'HEAD') {
    res.end();
    return;
  }
  if (req.url === undefined) {
    route.notFound(req, res);
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  const ctx = {url, route, serverId, gameLoader: GameLoader.getInstance()};
  const handler: IHandler | undefined = handlers.get(url.pathname);

  if (handler !== undefined) {
    handler.processRequest(req, res, ctx);
  } else if (req.method === 'GET' && url.pathname.startsWith('/terraforming/assets/')) {
    ServeAsset.INSTANCE.get(req, res, ctx);
  } else {
    route.notFound(req, res);
  }
}
