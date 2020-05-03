import { Game } from '../Game';
import { PlayerInput } from '../PlayerInput';
import { Player } from '../Player';
import { PlayerInterrupt } from './PlayerInterrupt';
import { OrOptions } from '../inputs/OrOptions';
import { SelectOption } from '../inputs/SelectOption';
import { LogMessageType } from "../LogMessageType";
import { LogMessageData } from "../LogMessageData";
import { LogMessageDataType } from "../LogMessageDataType";

export class SelectParty implements PlayerInterrupt {
    public playerInput: PlayerInput;
    constructor(
        public player: Player,
        public game: Game,
        public title: string = "Select where to send a delegate",
        public nbr: number = 1,
        public replace: "NEUTRAL" | Player | undefined = undefined,
        public price: number | undefined = undefined,
    ){
        const sendDelegate = new OrOptions();
        // Change the default title
        sendDelegate.title = title;
        let parties;
        if (replace) {
          parties = game.turmoil!.parties.filter(party => {
              if (party.delegates.length > 1) {
                return party.delegates.filter((delegate) => delegate !== party.partyLeader).indexOf(replace) !== -1
              } else {
                return false;
              }
          });
        }
        else {
          parties = game.turmoil!.parties;
        }
        sendDelegate.options = parties.map(party => new SelectOption(
              party.name + " - (" + party.description + ")", 
              () => {
                if (price) {
                  game.addSelectHowToPayInterrupt(player, price, false, false, "Select how to pay for send delegate action");
                }

                for (let i = 0; i < nbr; i++) {
                  if (replace) {
                    game.turmoil?.removeDelegateFromParty(replace, party.name, game);
                  }
                  game.turmoil?.sendDelegateToParty(player, party.name, game);
                }
                game.log(
                  LogMessageType.DEFAULT,
                  "${0} sent "+ nbr + " delegate in ${1} area",
                  new LogMessageData(LogMessageDataType.PLAYER, player.id),
                  new LogMessageData(LogMessageDataType.PARTY, party.name)
                );
                return undefined;
              }
            ));
        this.playerInput = sendDelegate;
    };
}    
