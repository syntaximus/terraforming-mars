
import { expect } from "chai";
import { CheungShingMARS } from "../../../src/cards/prelude/CheungShingMARS";
import { Color } from "../../../src/Color";
import { Game } from "../../../src/Game";
import { Player } from "../../../src/Player";
import { Ants } from "../../../src/cards/Ants";
import { BuildingIndustries } from "../../../src/cards/BuildingIndustries";

describe("CheungShingMARS", function () {
    it("Gets card discount", function () {
        const card = new CheungShingMARS();
        const player = new Player("test", Color.BLUE, false);
        const game = new Game("foobar", [player], player);
        const ants = new Ants();
        const buildingIndustries = new BuildingIndustries();
        expect(card.getCardDiscount(player, game, ants)).to.eq(0);
        expect(card.getCardDiscount(player, game, buildingIndustries)).to.eq(2);
    });
    it("Should play", function () {
        const card = new CheungShingMARS();
        const player = new Player("test", Color.BLUE, false);
        const action = card.play(player);
        expect(action).to.eq(undefined);
        expect(player.megaCreditProduction).to.eq(3);
    });
});
