import {expect} from 'chai';
import {getTestPlayer, newTestGame} from '../TestGame';
import {OrOptions} from '../../src/server/inputs/OrOptions';
import {TestPlayer} from '../TestPlayer';
import {SelectOption} from '../../src/server/inputs/SelectOption';
import {SelectAmount} from '../../src/server/inputs/SelectAmount';

describe('orOptions', function() {
  let player: TestPlayer;
  let value = -1;
  function cb(val: number) {
    value = val;
    return undefined;
  }

  beforeEach(() => {
    const game = newTestGame(1);
    player = getTestPlayer(game, 0);
  });

  it('Simple', function() {
    const orOptions = new OrOptions(
      new SelectOption('', '', () => {
        return cb(2);
      }),
      new SelectOption('', '', () => {
        return cb(3);
      }),
      new SelectAmount('', '', cb, 0, 10),
    );

    expect(orOptions.options).has.length(3);

    orOptions.process({type: 'or', index: 0, response: {type: 'option'}}, player);
    expect(value).eq(2);

    orOptions.process({type: 'or', index: 1, response: {type: 'option'}}, player);
    expect(value).eq(3);

    orOptions.process({type: 'or', index: 2, response: {type: 'amount', amount: 8}}, player);
    expect(value).eq(8);

    expect(() => {
      orOptions.process({type: 'or', index: 2, response: {type: 'option'}}, player);
    }).to.throw(/Not a valid SelectAmountResponse/);

    expect(() => {
      orOptions.process({type: 'or', index: 5, response: {type: 'option'}}, player);
    }).to.throw(/Invalid index/);

    expect(() => {
      orOptions.process({type: 'or', index: 3, response: {type: 'option'}}, player);
    }).to.throw(/Invalid index/);
  });
});
