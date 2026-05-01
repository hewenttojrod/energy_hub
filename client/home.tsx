/**
 * energy_hub module home/landing page.
 * Currently displays a simple welcome message.
 * Can be expanded with module overview, statistics, or quick actions.
 */
import FormBody from "@templates/form-body";

export default function EnergyHubHome() {
  return (
    <FormBody title="Energy Hub">
      <p className="body-text">
        Pulling in NYISO data for testing <br/> 
        <br/>
        useful links: <br/>
        <div>
          NYISO: < br/>
          <div>
            - <a className="underline" href="https://www.nyiso.com/real-time-dashboard" >Real time dash</a><br/>
            - <a className="underline" href="https://mis.nyiso.com/public/">Public data</a><br/>
          </div>
          <br/>
        </div>
        <div>
          <br/>
          <a className="underline" href="https://github.com/m4rz910/NYISOToolkit/tree/master">NYISOToolkit</a>
        </div>
      </p>
    </FormBody>
  );
}
