import { ReStreamSocket } from '@boatkit-io/restream';
import './App.css'
import BoardStore from './stores/BoardStore'

import SocketIoClient from 'socket.io-client';
import { withResubAutoSubscriptions } from '@boatkit-io/resub';
import { useEffect, useState } from 'react';
import { PlaceTokenRequest, ServerTimeEvent } from './restream/PackageGame';

const params = new URLSearchParams(window.location.search);
const connectionMode = params.get('mode') === 'relay' ? 'relay' : 'direct';
const serverURL = connectionMode === 'relay' ? 'http://localhost:8090' : 'http://localhost:8080';

const socket = SocketIoClient(serverURL, {
  path: '/socket',
  reconnection: true,
});

const rss = new ReStreamSocket(socket);

socket.on('connect', () => {
  // no auth, just send it
  rss.markAuthenticated();
});

socket.open();

// eslint-disable-next-line react-refresh/only-export-components
function App() {
  const board = BoardStore.getBoard();
  const xTurn = BoardStore.getXTurn();
  const nextToken = xTurn ? 'X' : 'O';
  const [lastServerTime, setLastServerTime] = useState<Date>();

  useEffect(() => rss.subscribeToEvent(ServerTimeEvent, (event) => {
    setLastServerTime(event.currentTime);
  }), []);

  return (
    <>
      <h1>Tic Tac Toe Relay</h1>
      <p>
        Connected to {connectionMode === 'relay' ? 'cloud relay on :8090' : 'direct server on :8080'}.
        {' '}
        <a href="?mode=direct">Direct</a>
        {' | '}
        <a href="?mode=relay">Relay</a>
      </p>
      <h2>Current Player: {nextToken}</h2>
      <h2>Last Server Time: {lastServerTime ? lastServerTime.toLocaleString() : 'waiting...'}</h2>
      <div className="board">
        <table align="center">
          <tbody>
            {board.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex} onClick={async () => { try { await rss.sendRPC(PlaceTokenRequest.fromValues(cellIndex, rowIndex)); } catch (error) { alert(error); } }}>{cell || ' '}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

export default withResubAutoSubscriptions(App)
