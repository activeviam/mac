import {
  ExtensionModule,
  pluginWidgetPivotTable,
} from "@activeviam/activeui-sdk";
import { withSandboxClients } from "@activeviam/sandbox-clients";
import _merge from "lodash/merge";
import { plugins } from "./plugins";

const extension: ExtensionModule = {
  activate: async (configuration) => {
    _merge(configuration.pluginRegistry, plugins);
    configuration.initialDashboardPageState = {
      content: { "0": pluginWidgetPivotTable.initialState },
      layout: {
        children: [
          {
            leafKey: "0",
            size: 1,
          },
        ],
        direction: "row",
      },
    };
    configuration.higherOrderComponents = [withSandboxClients];
  },
};

export default extension;
